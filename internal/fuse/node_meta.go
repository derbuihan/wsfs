package fuse

import (
	"context"
	"errors"
	iofs "io/fs"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/logging"
)

func fileInfoChanged(oldInfo, newInfo databricks.WSFileInfo) bool {
	sizeChanged := oldInfo.Size() != newInfo.Size()
	if oldInfo.IsNotebook() && newInfo.IsNotebook() && oldInfo.NotebookSizeComputed != newInfo.NotebookSizeComputed {
		sizeChanged = false
	}

	return oldInfo.ModifiedAt != newInfo.ModifiedAt ||
		sizeChanged ||
		oldInfo.ObjectId != newInfo.ObjectId ||
		oldInfo.ResourceId != newInfo.ResourceId ||
		oldInfo.Path != newInfo.Path
}

func (n *WSNode) metadataFreshLocked() bool {
	if n.metadataCheckedAt.IsZero() {
		return false
	}

	ttl := time.Second
	if n.wfClient != nil {
		ttl = n.wfClient.MetadataTTL()
	}
	if ttl <= 0 {
		ttl = time.Second
	}

	return time.Since(n.metadataCheckedAt) < ttl
}

func (n *WSNode) refreshMetadataLocked(ctx context.Context, bypassCache bool) (bool, syscall.Errno) {
	if n.isDirtyLocked() {
		return false, 0
	}
	if n.wfClient == nil {
		if n.metadataCheckedAt.IsZero() {
			n.metadataCheckedAt = time.Now()
		}
		return false, 0
	}

	if n.metadataCheckedAt.IsZero() {
		if !bypassCache {
			n.metadataCheckedAt = time.Now()
			return false, 0
		}
	}
	if !bypassCache && n.metadataFreshLocked() {
		return false, 0
	}
	if bypassCache {
		n.wfClient.CacheInvalidate(n.Path())
	}

	info, err := n.wfClient.Stat(ctx, n.Path())
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			return false, syscall.ENOENT
		}
		return false, errnoFromBackendError(backendOpLookup, err)
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("refreshMetadata: unexpected file info type for %s", n.Path())
		return false, syscall.EIO
	}

	changed := fileInfoChanged(n.fileInfo, wsInfo)
	if changed {
		oldPath := n.fileInfo.Path
		n.clearCleanBufferLocked()
		n.deleteDiskCacheEntries(oldPath, wsInfo.Path)
	}

	n.fileInfo = wsInfo
	n.metadataCheckedAt = time.Now()
	return changed, 0
}

func (n *WSNode) refreshMetadataIfNeededLocked(ctx context.Context) syscall.Errno {
	_, errno := n.refreshMetadataLocked(ctx, false)
	return errno
}

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

	// UID/GID are stable and reflect the mount owner, not the current caller.
	out.Uid = n.ownerUid
	out.Gid = n.ownerGid
}

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Getattr called on path: %s", n.Path())

	if errno := n.refreshMetadataIfNeededLocked(ctx); errno != 0 {
		return errno
	}

	n.fillAttr(ctx, &out.Attr)

	// When buffer is dirty, use local buffer size to ensure consistency
	// This prevents race conditions where stat sees intermediate state
	if n.isDirtyLocked() && n.buf.Data != nil {
		out.Attr.Size = uint64(len(n.buf.Data))
		out.Attr.Blocks = (out.Attr.Size + blockFactor - 1) / blockFactor
	}

	out.SetTimeout(n.attrTimeout())

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
		// chmod-style requests are accepted as a compatibility no-op.
		// Reported mode bits remain synthetic and stable.
	}
	if _, ok := in.GetUID(); ok {
		return syscall.ENOTSUP
	}
	if _, ok := in.GetGID(); ok {
		return syscall.ENOTSUP
	}
	sizeChanged := false
	mtimeRequested := false
	atimeRequested := false
	if _, ok := in.GetMTime(); ok {
		mtimeRequested = true
	}
	if _, ok := in.GetATime(); ok {
		atimeRequested = true
	}

	if size, ok := in.GetSize(); ok {
		if n.fileInfo.IsDir() {
			return syscall.EISDIR
		}
		if size > 0 && n.buf.Data == nil {
			if errno := n.ensureDataForMutationLocked(ctx); errno != 0 {
				return errno
			}
		}
		n.truncateLocked(size)
		sizeChanged = true
	}

	if !sizeChanged && (atimeRequested || mtimeRequested) {
		if n.allowPostCreateTimestamps && n.openCount > 0 && !n.isDirtyLocked() && n.fileInfo.Size() == 0 {
			n.fillAttr(ctx, &out.Attr)
			return 0
		}
		return syscall.ENOTSUP
	}

	if sizeChanged {
		n.markModifiedLocked(time.Now())
		n.metadataCheckedAt = time.Now()
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
	}

	n.fillAttr(ctx, &out.Attr)

	return 0
}
