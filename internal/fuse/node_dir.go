package fuse

import (
	"context"
	"fmt"
	"path"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/logging"
	"wsfs/internal/pathutil"
)

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
		if wsEntry, ok := e.(databricks.WSDirEntry); ok {
			name = pathutil.ToFuseName(name, wsEntry.IsNotebook())
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

	// Check if we already have this inode with a dirty buffer.
	// If so, use its state instead of fetching from Databricks to avoid
	// race conditions where stat sees intermediate state during writes.
	existingChild := n.GetChild(name)
	if existingChild != nil {
		existingNode, ok := existingChild.Operations().(*WSNode)
		if ok {
			existingNode.mu.Lock()
			if existingNode.isDirtyLocked() {
				// Use existing node's state - it has uncommitted changes
				existingNode.fillAttr(ctx, &out.Attr)
				if existingNode.buf.Data != nil {
					out.Attr.Size = uint64(len(existingNode.buf.Data))
					out.Attr.Blocks = (out.Attr.Size + blockFactor - 1) / blockFactor
				}
				existingNode.mu.Unlock()
				out.SetEntryTimeout(entryTimeoutSec)
				out.SetAttrTimeout(attrTimeoutSec)
				logging.Debugf("Lookup: returning existing dirty node for %s", childPath)
				return existingChild, 0
			}
			existingNode.mu.Unlock()
		}
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

func (n *WSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	logging.Debugf("Create called in dir: %s, for file: %s", n.Path(), name)

	childPath, err := validateChildPath(n.Path(), name)
	if err != nil {
		logging.Debugf("Create: invalid path: %v", err)
		return nil, nil, 0, syscall.EINVAL
	}

	// For .ipynb files, create an empty Jupyter notebook
	var initialContent []byte
	if pathutil.HasNotebookSuffix(name) {
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
	childNode.incrementOpenLocked()
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

	// Remove from cache (use remote path without .ipynb suffix)
	actualPath := pathutil.ToRemotePath(childPath)
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

	// Delete old path from cache (use remote path without .ipynb suffix)
	actualOldPath := pathutil.ToRemotePath(oldPath)
	actualNewPath := pathutil.ToRemotePath(newPath)
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		if err := n.diskCache.Delete(actualOldPath); err != nil {
			logging.Debugf("Failed to delete old path from cache %s: %v", actualOldPath, err)
		}
	}

	childInode := n.GetChild(name)
	if childInode != nil {
		updateSubtreePaths(childInode, actualOldPath, actualNewPath)
	}

	return 0
}

func updateSubtreePaths(inode *fs.Inode, oldPrefix, newPrefix string) {
	if inode == nil {
		return
	}

	if node, ok := inode.Operations().(*WSNode); ok {
		node.mu.Lock()
		if pathHasPrefix(node.fileInfo.Path, oldPrefix) {
			oldPath := node.fileInfo.Path
			rel := strings.TrimPrefix(oldPath, oldPrefix)
			node.fileInfo.Path = newPrefix + rel
			logging.Debugf("Updating internal path for in-memory node from '%s' to '%s'", oldPath, node.fileInfo.Path)
		}
		node.mu.Unlock()
	}

	children := inode.Children()
	for _, child := range children {
		updateSubtreePaths(child, oldPrefix, newPrefix)
	}
}

func pathHasPrefix(path, prefix string) bool {
	if prefix == "/" {
		return strings.HasPrefix(path, "/")
	}
	return path == prefix || strings.HasPrefix(path, prefix+"/")
}

func (n *WSNode) OnForget() {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("OnForget called on path: %s", n.fileInfo.Path)

	if n.isDirtyLocked() {
		return
	}
	n.resetBufferLocked()
}
