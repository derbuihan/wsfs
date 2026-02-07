package fuse

import (
	"context"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/logging"
)

func (n *WSNode) fillAttr(ctx context.Context, out *fuse.Attr) {
	wsInfo := n.fileInfo

	// Set the attributes for the file or directory
	if wsInfo.IsDir() {
		out.Mode = syscall.S_IFDIR | dirMode
		out.Nlink = dirNlink
	} else {
		out.Mode = syscall.S_IFREG | fileMode
		out.Nlink = fileNlink
	}

	// Block size
	out.Size = uint64(wsInfo.Size())
	out.Blksize = blockSize
	out.Blocks = (out.Size + blockFactor - 1) / blockFactor

	// Timestamp
	modTime := wsInfo.ModTime()
	out.Mtime = uint64(modTime.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime

	// UID/GID
	caller, ok := fuse.FromContext(ctx)
	if ok {
		out.Uid = caller.Uid
		out.Gid = caller.Gid
	}
}

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Getattr called on path: %s", n.Path())

	n.fillAttr(ctx, &out.Attr)

	// When buffer is dirty, use local buffer size to ensure consistency
	// This prevents race conditions where stat sees intermediate state
	if n.isDirtyLocked() && n.buf.Data != nil {
		out.Attr.Size = uint64(len(n.buf.Data))
		out.Attr.Blocks = (out.Attr.Size + blockFactor - 1) / blockFactor
	}

	out.SetTimeout(attrTimeoutSec)

	return 0
}

func (n *WSNode) Access(ctx context.Context, mask uint32) syscall.Errno {
	logging.Debugf("Access called on path: %s (mask: %d)", n.Path(), mask)

	// Enforce UID-based access control when restrictAccess is enabled
	if n.restrictAccess {
		caller, ok := fuse.FromContext(ctx)
		if !ok {
			logging.Warnf("Access: failed to get caller context for %s", n.Path())
			return syscall.EACCES
		}
		if caller.Uid != n.ownerUid {
			logging.Debugf("Access denied: caller UID %d != owner UID %d for %s", caller.Uid, n.ownerUid, n.Path())
			return syscall.EACCES
		}
	}

	return 0
}

func (n *WSNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	logging.Debugf("Statfs called on path: %s", n.Path())

	const blockSize = uint32(4096)
	const totalBlocks = uint64(1 << 30)
	const totalFiles = uint64(1 << 24)

	out.Bsize = blockSize
	out.Frsize = blockSize
	out.Blocks = totalBlocks
	out.Bfree = totalBlocks
	out.Bavail = totalBlocks
	out.Files = totalFiles
	out.Ffree = totalFiles
	out.NameLen = maxNameLen

	return 0
}

func (n *WSNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Setattr called on path: %s", n.fileInfo.Path)

	if _, ok := in.GetMode(); ok {
		return syscall.ENOTSUP
	}
	if _, ok := in.GetUID(); ok {
		return syscall.ENOTSUP
	}
	if _, ok := in.GetGID(); ok {
		return syscall.ENOTSUP
	}
	var mtime *time.Time
	sizeChanged := false
	atimeRequested := false
	if t, ok := in.GetMTime(); ok {
		mtime = &t
	}
	if _, ok := in.GetATime(); ok {
		atimeRequested = true
	}

	if size, ok := in.GetSize(); ok {
		if n.fileInfo.IsDir() {
			return syscall.EISDIR
		}
		if size > 0 && n.buf.Data == nil {
			if errno := n.ensureDataLocked(ctx); errno != 0 {
				return errno
			}
		}
		n.truncateLocked(size)
		sizeChanged = true
		if mtime == nil {
			now := time.Now()
			mtime = &now
		}
	}

	if atimeRequested && mtime == nil && !sizeChanged {
		return syscall.ENOTSUP
	}

	if mtime != nil {
		n.markModifiedLocked(*mtime)
	}

	if sizeChanged {
		// Defer flush to Flush/Release/Fsync to avoid race condition
		// where stat polling retrieves intermediate empty data from Databricks.
		// Invalidate metadata cache to prevent stale reads.
		n.wfClient.CacheInvalidate(n.Path())
		if n.shouldFlushNowLocked() {
			if errno := n.flushLocked(ctx); errno != 0 {
				return errno
			}
		}
	} else if mtime != nil {
		n.wfClient.CacheSet(n.Path(), n.fileInfo)
	}

	n.fillAttr(ctx, &out.Attr)

	return 0
}
