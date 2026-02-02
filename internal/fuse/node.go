package fuse

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	iofs "io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	"wsfs/internal/logging"
)

// File system constants
const (
	// Attribute and entry cache timeouts in seconds
	attrTimeoutSec  = 60
	entryTimeoutSec = 60

	// File permissions
	dirMode  = 0755
	fileMode = 0644

	// Block size for file attributes
	blockSize   = 4096
	blockFactor = 512 // for calculating number of blocks

	// Statfs limits
	maxNameLen = 255

	// Default inode number when no ID is available
	defaultIno = 1

	// Nlink values
	dirNlink  = 2
	fileNlink = 1
)

// Operation timeouts for API calls
const (
	// dataOpTimeout is used for read/write operations that may involve large files
	dataOpTimeout = 2 * time.Minute

	// metadataOpTimeout is used for stat, delete, mkdir, rename operations
	metadataOpTimeout = 30 * time.Second

	// dirListTimeout is used for directory listing operations
	dirListTimeout = 1 * time.Minute
)

// fileBuffer holds in-memory file data and dirty state.
type fileBuffer struct {
	Data  []byte
	Dirty bool
}

// NodeConfig holds configuration for access control.
type NodeConfig struct {
	OwnerUid       uint32 // UID of the user who mounted the filesystem
	RestrictAccess bool   // Whether to enforce UID-based access control
}

type WSNode struct {
	fs.Inode
	wfClient       databricks.WorkspaceFilesAPI
	diskCache      *filecache.DiskCache
	fileInfo       databricks.WSFileInfo
	buf            fileBuffer
	mu             sync.Mutex
	registry       *DirtyNodeRegistry
	ownerUid       uint32 // UID of the mount owner
	restrictAccess bool   // Enforce access control when true
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeSetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeOpendirer)((*WSNode)(nil))
var _ = (fs.NodeOpendirHandler)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))
var _ = (fs.NodeWriter)((*WSNode)(nil))
var _ = (fs.NodeFlusher)((*WSNode)(nil))
var _ = (fs.NodeFsyncer)((*WSNode)(nil))
var _ = (fs.NodeReleaser)((*WSNode)(nil))
var _ = (fs.NodeCreater)((*WSNode)(nil))
var _ = (fs.NodeUnlinker)((*WSNode)(nil))
var _ = (fs.NodeMkdirer)((*WSNode)(nil))
var _ = (fs.NodeRmdirer)((*WSNode)(nil))
var _ = (fs.NodeRenamer)((*WSNode)(nil))
var _ = (fs.NodeAccesser)((*WSNode)(nil))
var _ = (fs.NodeStatfser)((*WSNode)(nil))
var _ = (fs.NodeOnForgetter)((*WSNode)(nil))

func (n *WSNode) Path() string {
	return n.fileInfo.Path
}

func stableIno(info databricks.WSFileInfo) uint64 {
	if info.ObjectId > 0 {
		return uint64(info.ObjectId)
	}
	if info.ResourceId != "" {
		return hashStringToIno(info.ResourceId)
	}
	if info.Path != "" {
		return hashStringToIno(info.Path)
	}
	return defaultIno
}

func hashStringToIno(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	sum := h.Sum64()
	if sum == 0 {
		return defaultIno
	}
	return sum
}

// validateChildPath validates and constructs a child path, preventing path traversal attacks.
// Returns the validated child path or an error if the name contains path traversal sequences.
func validateChildPath(parentPath, childName string) (string, error) {
	// Reject names containing path separators or traversal sequences
	if strings.Contains(childName, "/") || strings.Contains(childName, "\\") {
		return "", fmt.Errorf("invalid child name: contains path separator")
	}
	if childName == "." || childName == ".." {
		return "", fmt.Errorf("invalid child name: %s", childName)
	}

	// Construct and clean the path
	childPath := path.Join(parentPath, childName)
	cleanPath := path.Clean(childPath)

	// Verify the result is actually a child of the parent
	cleanParent := path.Clean(parentPath)
	// Handle root path specially
	if cleanParent == "/" {
		if !strings.HasPrefix(cleanPath, "/") || cleanPath == "/" {
			return "", fmt.Errorf("path traversal detected")
		}
	} else {
		if !strings.HasPrefix(cleanPath, cleanParent+"/") {
			return "", fmt.Errorf("path traversal detected")
		}
	}

	return cleanPath, nil
}

func (n *WSNode) ensureDataLocked(ctx context.Context) syscall.Errno {
	if n.buf.Data != nil {
		return 0
	}
	if n.fileInfo.IsDir() {
		return syscall.EISDIR
	}

	remotePath := n.Path()
	remoteModTime := n.fileInfo.ModTime()

	// Try to get from cache first
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		cachedPath, found := n.diskCache.Get(remotePath, remoteModTime)
		if found {
			// Read from cached file
			data, err := os.ReadFile(cachedPath)
			if err == nil {
				logging.Debugf("Cache hit for %s", remotePath)
				n.buf.Data = data
				return 0
			}
			// Cache read failed, fall through to remote read
			logging.Debugf("Cache read failed for %s: %v", remotePath, err)
		}
	}

	// Cache miss or disabled - read from remote
	logging.Debugf("Cache miss for %s, fetching from remote", remotePath)
	readCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()
	data, err := n.wfClient.ReadAll(readCtx, remotePath)
	if err != nil {
		logging.Debugf("Failed to read file %s: %v", remotePath, err)
		if errors.Is(err, iofs.ErrNotExist) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}
	n.buf.Data = data

	// Store in cache
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		_, err := n.diskCache.Set(remotePath, data, remoteModTime)
		if err != nil {
			// Log error but don't fail the operation
			logging.Debugf("Failed to cache file %s: %v", remotePath, err)
		} else {
			logging.Debugf("Cached file %s (%d bytes)", remotePath, len(data))
		}
	}

	return 0
}

func (n *WSNode) truncateLocked(size uint64) {
	if n.buf.Data == nil {
		n.buf.Data = []byte{}
	}
	cur := uint64(len(n.buf.Data))
	if cur > size {
		n.buf.Data = n.buf.Data[:size]
	} else if cur < size {
		newData := make([]byte, size)
		copy(newData, n.buf.Data)
		n.buf.Data = newData
	}
	n.fileInfo.ObjectInfo.Size = int64(size)
	n.buf.Dirty = true
	if n.registry != nil {
		n.registry.Register(n)
	}
}

func (n *WSNode) markModifiedLocked(t time.Time) {
	n.fileInfo.ObjectInfo.ModifiedAt = t.UnixMilli()
}

func (n *WSNode) flushLocked(ctx context.Context) syscall.Errno {
	if !n.buf.Dirty || n.buf.Data == nil {
		return 0
	}

	// Apply timeout for write and stat operations
	opCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()

	remotePath := n.Path()
	err := n.wfClient.Write(opCtx, remotePath, n.buf.Data)
	if err != nil {
		logging.Warnf("Error writing back on Flush for %s: %v", remotePath, err)
		return syscall.EIO
	}
	n.buf.Dirty = false
	if n.registry != nil {
		n.registry.Unregister(n)
	}

	info, err := n.wfClient.Stat(opCtx, remotePath)
	if err != nil {
		logging.Warnf("Error refreshing file info after Flush for %s: %v", remotePath, err)
		return 0
	}
	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Warnf("Unexpected file info type after Flush for %s", remotePath)
		return 0
	}
	n.fileInfo = wsInfo

	// Update cache with new content
	if n.diskCache != nil && !n.diskCache.IsDisabled() && n.buf.Data != nil {
		_, err := n.diskCache.Set(remotePath, n.buf.Data, n.fileInfo.ModTime())
		if err != nil {
			logging.Debugf("Failed to update cache after flush for %s: %v", remotePath, err)
		} else {
			logging.Debugf("Updated cache after flush for %s", remotePath)
		}
	}

	return 0
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

	// UID/GID
	caller, ok := fuse.FromContext(ctx)
	if ok {
		out.Uid = caller.Uid
		out.Gid = caller.Gid
	}
}

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	logging.Debugf("Getattr called on path: %s", n.Path())

	n.fillAttr(ctx, &out.Attr)
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
		if errno := n.flushLocked(ctx); errno != 0 {
			return errno
		}
	} else if mtime != nil {
		n.wfClient.CacheSet(n.Path(), n.fileInfo)
	}

	n.fillAttr(ctx, &out.Attr)

	return 0
}

func (n *WSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	logging.Debugf("Readdir called on path: %s", n.Path())

	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	opCtx, cancel := context.WithTimeout(ctx, dirListTimeout)
	defer cancel()
	entries, err := n.wfClient.ReadDir(opCtx, n.Path())
	if err != nil {
		logging.Warnf("Error reading directory %s: %v", n.Path(), err)
		return nil, syscall.EIO
	}

	fuseEntries := make([]fuse.DirEntry, len(entries))
	for i, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		name := e.Name()
		// Add .ipynb extension for notebooks
		if wsEntry, ok := e.(databricks.WSDirEntry); ok && wsEntry.IsNotebook() {
			name = name + ".ipynb"
		}
		fuseEntries[i] = fuse.DirEntry{Name: name, Mode: mode}
	}

	return fs.NewListDirStream(fuseEntries), 0
}

func (n *WSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debugf("Lookup called on path: %s/%s", n.Path(), name)
	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Lookup: invalid path: %v", err)
		return nil, syscall.EINVAL
	}

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()
	info, err := n.wfClient.Stat(opCtx, childPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Lookup: unexpected file info type for %s", childPath)
		return nil, syscall.EIO
	}

	childNode := &WSNode{
		wfClient:       n.wfClient,
		diskCache:      n.diskCache,
		fileInfo:       wsInfo,
		registry:       n.registry,
		ownerUid:       n.ownerUid,
		restrictAccess: n.restrictAccess,
	}
	childNode.fillAttr(ctx, &out.Attr)

	out.SetEntryTimeout(entryTimeoutSec)
	out.SetAttrTimeout(attrTimeoutSec)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode), Ino: stableIno(wsInfo)})
	return child, 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Open called on path: %s", n.fileInfo.Path)

	if n.fileInfo.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	// Check for remote modifications before using cached data
	if n.buf.Data != nil && !n.buf.Dirty {
		info, err := n.wfClient.Stat(ctx, n.fileInfo.Path)
		if err == nil {
			wsInfo, ok := info.(databricks.WSFileInfo)
			if ok && wsInfo.ModTime().After(n.fileInfo.ModTime()) {
				// Remote file was modified, invalidate cache
				logging.Debugf("Remote file modified, invalidating cache for %s", n.fileInfo.Path)
				n.buf.Data = nil
				n.fileInfo = wsInfo
				// Also invalidate disk cache
				if n.diskCache != nil && !n.diskCache.IsDisabled() {
					actualPath := strings.TrimSuffix(n.fileInfo.Path, ".ipynb")
					n.diskCache.Delete(actualPath)
				}
			}
		}
	}

	if flags&syscall.O_TRUNC != 0 {
		n.buf.Data = []byte{}
		n.fileInfo.ObjectInfo.Size = 0
		n.markModifiedLocked(time.Now())
		n.buf.Dirty = true
		if n.registry != nil {
			n.registry.Register(n)
		}
	} else if n.buf.Data == nil {
		if errno := n.ensureDataLocked(ctx); errno != 0 {
			return nil, 0, errno
		}
	}

	openFlags := uint32(0)
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0 {
		openFlags |= fuse.FOPEN_DIRECT_IO
	} else {
		openFlags |= fuse.FOPEN_KEEP_CACHE
	}

	return nil, openFlags, 0
}

func (n *WSNode) Opendir(ctx context.Context) syscall.Errno {
	logging.Debugf("Opendir called on path: %s", n.Path())

	if !n.fileInfo.IsDir() {
		return syscall.ENOTDIR
	}

	return 0
}

func (n *WSNode) OpendirHandle(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	logging.Debugf("OpendirHandle called on path: %s", n.Path())

	if !n.fileInfo.IsDir() {
		return nil, 0, syscall.ENOTDIR
	}

	handle := &dirStreamHandle{
		creator: func(ctx context.Context) (fs.DirStream, syscall.Errno) {
			return n.Readdir(ctx)
		},
	}

	return handle, 0, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Read called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(dest))

	if n.buf.Data == nil {
		if errno := n.ensureDataLocked(ctx); errno != 0 {
			return nil, errno
		}
	}

	end := off + int64(len(dest))
	if end > int64(len(n.buf.Data)) {
		end = int64(len(n.buf.Data))
	}

	if off >= int64(len(n.buf.Data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	result := n.buf.Data[off:end]
	return fuse.ReadResultData(result), 0
}

func (n *WSNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Write called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(data))
	if off < 0 {
		return 0, syscall.EINVAL
	}
	if n.buf.Data == nil {
		if errno := n.ensureDataLocked(ctx); errno != 0 {
			return 0, errno
		}
	}

	end := off + int64(len(data))
	if int64(len(n.buf.Data)) < end {
		newData := make([]byte, end)
		copy(newData, n.buf.Data)
		n.buf.Data = newData
	}
	copy(n.buf.Data[off:], data)

	n.fileInfo.ObjectInfo.Size = int64(len(n.buf.Data))
	n.markModifiedLocked(time.Now())
	n.buf.Dirty = true
	if n.registry != nil {
		n.registry.Register(n)
	}

	return uint32(len(data)), 0
}

func (n *WSNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Flush called on path: %s", n.fileInfo.Path)
	return n.flushLocked(ctx)
}

func (n *WSNode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Fsync called on path: %s", n.fileInfo.Path)
	return n.flushLocked(ctx)
}

func (n *WSNode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Release called on path: %s", n.fileInfo.Path)
	errno := n.flushLocked(ctx)
	if errno == 0 {
		n.buf.Data = nil
		n.buf.Dirty = false
	}

	return errno
}

func (n *WSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	logging.Debugf("Create called in dir: %s, for file: %s", n.Path(), name)

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Create: invalid path: %v", err)
		return nil, nil, 0, syscall.EINVAL
	}

	// For .ipynb files, create an empty Jupyter notebook
	var initialContent []byte
	if strings.HasSuffix(name, ".ipynb") {
		initialContent = []byte(`{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":4}`)
	} else {
		initialContent = []byte{}
	}

	opCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()

	err = n.wfClient.Write(opCtx, childPath, initialContent)
	if err != nil {
		logging.Warnf("Error creating file %s: %v", childPath, err)
		return nil, nil, 0, syscall.EIO
	}

	info, err := n.wfClient.Stat(opCtx, childPath)
	if err != nil {
		logging.Warnf("Error stating new file %s: %v", childPath, err)
		return nil, nil, 0, syscall.EIO
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Create: unexpected file info type for %s", childPath)
		return nil, nil, 0, syscall.EIO
	}
	childNode := &WSNode{
		wfClient:       n.wfClient,
		diskCache:      n.diskCache,
		fileInfo:       wsInfo,
		buf:            fileBuffer{Data: initialContent},
		registry:       n.registry,
		ownerUid:       n.ownerUid,
		restrictAccess: n.restrictAccess,
	}
	childNode.fillAttr(ctx, &out.Attr)

	out.SetEntryTimeout(entryTimeoutSec)
	out.SetAttrTimeout(attrTimeoutSec)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode), Ino: stableIno(wsInfo)})
	return child, nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	logging.Debugf("Unlink called in dir: %s, for file: %s", n.Path(), name)

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Unlink: invalid path: %v", err)
		return syscall.EINVAL
	}

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()

	info, err := n.wfClient.Stat(opCtx, childPath)
	if err != nil {
		return syscall.ENOENT
	}

	if info.IsDir() {
		return syscall.EISDIR
	}

	err = n.wfClient.Delete(opCtx, childPath, false)
	if err != nil {
		logging.Warnf("Error deleting file %s: %v", childPath, err)
		return syscall.EIO
	}

	// Remove from cache (use actual path without .ipynb suffix)
	actualPath := strings.TrimSuffix(childPath, ".ipynb")
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		if err := n.diskCache.Delete(actualPath); err != nil {
			logging.Debugf("Failed to delete from cache %s: %v", actualPath, err)
		}
	}

	return 0
}

func (n *WSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	logging.Debugf("Mkdir called in dir: %s, for new dir: %s", n.Path(), name)

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Mkdir: invalid path: %v", err)
		return nil, syscall.EINVAL
	}

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()

	err = n.wfClient.Mkdir(opCtx, childPath)
	if err != nil {
		logging.Warnf("Error creating directory %s: %v", childPath, err)
		return nil, syscall.EIO
	}

	info, err := n.wfClient.Stat(opCtx, childPath)
	if err != nil {
		logging.Warnf("Error stating new directory %s: %v", childPath, err)
		return nil, syscall.EIO
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Mkdir: unexpected file info type for %s", childPath)
		return nil, syscall.EIO
	}
	childNode := &WSNode{
		wfClient:       n.wfClient,
		diskCache:      n.diskCache,
		fileInfo:       wsInfo,
		registry:       n.registry,
		ownerUid:       n.ownerUid,
		restrictAccess: n.restrictAccess,
	}
	childNode.fillAttr(ctx, &out.Attr)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode), Ino: stableIno(wsInfo)})
	return child, 0
}

func (n *WSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	logging.Debugf("Rmdir called in dir: %s, for dir: %s", n.Path(), name)

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Rmdir: invalid path: %v", err)
		return syscall.EINVAL
	}

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()

	info, err := n.wfClient.Stat(opCtx, childPath)
	if err != nil {
		return syscall.ENOENT
	}
	if !info.IsDir() {
		return syscall.ENOTDIR
	}

	err = n.wfClient.Delete(opCtx, childPath, false)
	if err != nil {
		logging.Warnf("Error deleting directory %s: %v", childPath, err)
		return syscall.EIO
	}

	return 0
}

func (n *WSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	logging.Debugf("Rename called from %s to %s", name, newName)

	newParentNode, ok := newParent.EmbeddedInode().Operations().(*WSNode)
	if !ok {
		logging.Debugf("Rename: failed to get parent node for %s", newName)
		return syscall.EIO
	}

	oldPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Rename: invalid old path: %v", err)
		return syscall.EINVAL
	}

	newPath, err := validateChildPath(newParentNode.fileInfo.Path, newName)
	if err != nil {
		logging.Debugf("Rename: invalid new path: %v", err)
		return syscall.EINVAL
	}

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()
	err = n.wfClient.Rename(opCtx, oldPath, newPath)
	if err != nil {
		logging.Warnf("Error renaming %s to %s: %v", oldPath, newPath, err)
		return syscall.EIO
	}

	// Delete old path from cache (use actual path without .ipynb suffix)
	actualOldPath := strings.TrimSuffix(oldPath, ".ipynb")
	actualNewPath := strings.TrimSuffix(newPath, ".ipynb")
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		if err := n.diskCache.Delete(actualOldPath); err != nil {
			logging.Debugf("Failed to delete old path from cache %s: %v", actualOldPath, err)
		}
	}

	childInode := n.GetChild(name)
	if childInode != nil {
		childNode, ok := childInode.Operations().(*WSNode)
		if ok {
			childNode.mu.Lock()
			logging.Debugf("Updating internal path for in-memory node from '%s' to '%s'", childNode.fileInfo.Path, actualNewPath)
			childNode.fileInfo.Path = actualNewPath
			childNode.mu.Unlock()
		}
	}

	return 0
}

func (n *WSNode) OnForget() {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("OnForget called on path: %s", n.fileInfo.Path)

	if n.buf.Dirty {
		return
	}
	n.buf.Data = nil
	n.buf.Dirty = false
}

func NewRootNode(wfClient databricks.WorkspaceFilesAPI, diskCache *filecache.DiskCache, rootPath string, registry *DirtyNodeRegistry, config *NodeConfig) (*WSNode, error) {
	info, err := wfClient.Stat(context.Background(), rootPath)

	if err != nil {
		return nil, err
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected file info type for root path %s", rootPath)
	}
	if !wsInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	node := &WSNode{
		wfClient:  wfClient,
		diskCache: diskCache,
		fileInfo:  wsInfo,
		registry:  registry,
	}

	// Apply access control configuration
	if config != nil {
		node.ownerUid = config.OwnerUid
		node.restrictAccess = config.RestrictAccess
	}

	return node, nil
}
