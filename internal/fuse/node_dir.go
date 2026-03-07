package fuse

import (
	"context"
	"fmt"
	"path"
	"strings"
	"syscall"
	"time"

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

func notebookVisibleEntryName(info databricks.WSFileInfo, usedNames map[string]struct{}) (string, bool) {
	preferred := pathutil.NotebookVisibleName(info.Name(), info.Language)
	if _, exists := usedNames[preferred]; !exists {
		usedNames[preferred] = struct{}{}
		return preferred, true
	}

	fallback := pathutil.NotebookFallbackName(info.Name())
	if _, exists := usedNames[fallback]; exists {
		logging.Debugf("Readdir: hiding notebook %s because both %s and %s collide", info.Path, preferred, fallback)
		return "", false
	}

	usedNames[fallback] = struct{}{}
	return fallback, true
}

func renameTargetPath(sourceInfo databricks.WSFileInfo, visiblePath string) string {
	if sourceInfo.IsNotebook() {
		if actualPath, _, ok := pathutil.NotebookRemotePathFromSourcePath(visiblePath); ok {
			return actualPath
		}
		if actualPath, ok := pathutil.NotebookRemotePathFromFallbackPath(visiblePath); ok {
			return actualPath
		}
	}
	return visiblePath
}

func notebookRenameChangesLanguage(sourceInfo databricks.WSFileInfo, visiblePath string) bool {
	if !sourceInfo.IsNotebook() {
		return false
	}

	_, targetLanguage, ok := pathutil.NotebookRemotePathFromSourcePath(visiblePath)
	if !ok {
		return false
	}

	return targetLanguage != sourceInfo.Language
}

func flushRenameChildIfDirty(ctx context.Context, inode *fs.Inode) syscall.Errno {
	if inode == nil {
		return 0
	}

	node, ok := inode.Operations().(*WSNode)
	if !ok {
		return 0
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	if !node.isDirtyLocked() {
		return 0
	}

	return node.flushLocked(ctx)
}

func refreshRenamedNode(ctx context.Context, wfClient databricks.WorkspaceFilesAPI, inode *fs.Inode, visiblePath string, actualPath string) {
	if inode == nil {
		return
	}

	node, ok := inode.Operations().(*WSNode)
	if !ok {
		return
	}

	info, err := wfClient.Stat(ctx, visiblePath)
	if err != nil {
		info, err = wfClient.Stat(ctx, actualPath)
		if err != nil {
			logging.Debugf("Rename: failed to refresh node info for %s: %v", visiblePath, err)
			return
		}
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Rename: unexpected refreshed file info type for %s", visiblePath)
		return
	}

	node.mu.Lock()
	defer node.mu.Unlock()

	node.fileInfo = wsInfo
	node.metadataCheckedAt = time.Now()
	node.resetBufferLocked()
	node.buf.ReplaceOnFirstWrite = false
}

func notifyContentIfPossible(inode *fs.Inode, path string) {
	if inode == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	if errno := inode.NotifyContent(0, 0); errno != 0 {
		logging.Debugf("Rename: failed to invalidate kernel content cache for %s: %v", path, errno)
	}
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
		return nil, errnoFromBackendError(backendOpReadDir, err)
	}

	fuseEntries := make([]fuse.DirEntry, 0, len(entries))
	usedNames := make(map[string]struct{}, len(entries))

	for _, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		wsEntry, ok := e.(databricks.WSDirEntry)
		if ok && wsEntry.IsNotebook() {
			continue
		}
		name := e.Name()
		usedNames[name] = struct{}{}
		fuseEntries = append(fuseEntries, fuse.DirEntry{Name: name, Mode: mode})
	}

	for _, e := range entries {
		wsEntry, ok := e.(databricks.WSDirEntry)
		if !ok || !wsEntry.IsNotebook() {
			continue
		}

		name, visible := notebookVisibleEntryName(wsEntry.WSFileInfo, usedNames)
		if !visible {
			continue
		}
		fuseEntries = append(fuseEntries, fuse.DirEntry{Name: name, Mode: uint32(syscall.S_IFREG)})
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
		return nil, errnoFromBackendError(backendOpLookup, err)
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Lookup: unexpected file info type for %s", childPath)
		return nil, syscall.EIO
	}

	childNode := &WSNode{
		wfClient:          n.wfClient,
		diskCache:         n.diskCache,
		fileInfo:          wsInfo,
		registry:          n.registry,
		ownerUid:          n.ownerUid,
		restrictAccess:    n.restrictAccess,
		metadataCheckedAt: time.Now(),
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

	var initialContent []byte
	if _, language, ok := pathutil.NotebookRemotePathFromSourcePath(name); ok {
		initialContent = []byte(pathutil.NotebookSourceHeader(language) + "\n")
	}

	opCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()

	err = n.wfClient.Write(opCtx, childPath, initialContent)
	if err != nil {
		logging.Warnf("Error creating file %s: %v", childPath, err)
		return nil, nil, 0, errnoFromBackendError(backendOpCreate, err)
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
		wfClient:          n.wfClient,
		diskCache:         n.diskCache,
		fileInfo:          wsInfo,
		buf:               fileBuffer{Data: initialContent, ReplaceOnFirstWrite: len(initialContent) > 0},
		registry:          n.registry,
		ownerUid:          n.ownerUid,
		restrictAccess:    n.restrictAccess,
		metadataCheckedAt: time.Now(),
	}
	childNode.incrementOpenLocked()
	childNode.fillAttr(ctx, &out.Attr)

	out.SetEntryTimeout(entryTimeoutSec)
	out.SetAttrTimeout(attrTimeoutSec)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode), Ino: stableIno(wsInfo)})
	return child, &wsFileHandle{}, fuse.FOPEN_KEEP_CACHE, 0
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
		return errnoFromBackendError(backendOpDelete, err)
	}

	if info.IsDir() {
		return syscall.EISDIR
	}

	err = n.wfClient.Delete(opCtx, childPath, false)
	if err != nil {
		logging.Warnf("Error deleting file %s: %v", childPath, err)
		return errnoFromBackendError(backendOpDelete, err)
	}

	actualPath := childPath
	if wsInfo, ok := info.(databricks.WSFileInfo); ok {
		actualPath = wsInfo.Path
	}
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
		return nil, errnoFromBackendError(backendOpMkdir, err)
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
		wfClient:          n.wfClient,
		diskCache:         n.diskCache,
		fileInfo:          wsInfo,
		registry:          n.registry,
		ownerUid:          n.ownerUid,
		restrictAccess:    n.restrictAccess,
		metadataCheckedAt: time.Now(),
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
		return errnoFromBackendError(backendOpDeleteDir, err)
	}
	if !info.IsDir() {
		return syscall.ENOTDIR
	}

	err = n.wfClient.Delete(opCtx, childPath, false)
	if err != nil {
		logging.Warnf("Error deleting directory %s: %v", childPath, err)
		return errnoFromBackendError(backendOpDeleteDir, err)
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

	childInode := n.GetChild(name)

	opCtx, cancel := context.WithTimeout(ctx, metadataOpTimeout)
	defer cancel()
	info, err := n.wfClient.Stat(opCtx, oldPath)
	if err != nil {
		return errnoFromBackendError(backendOpRename, err)
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Debugf("Rename: unexpected file info type for %s", oldPath)
		return syscall.EIO
	}

	languageChanged := notebookRenameChangesLanguage(wsInfo, newPath)
	if languageChanged {
		flushCtx, flushCancel := context.WithTimeout(ctx, dataOpTimeout)
		defer flushCancel()
		if errno := flushRenameChildIfDirty(flushCtx, childInode); errno != 0 {
			logging.Warnf("Error flushing dirty notebook before rename %s -> %s: %v", oldPath, newPath, errno)
			return errno
		}
	}

	err = n.wfClient.Rename(opCtx, oldPath, newPath)
	if err != nil {
		logging.Warnf("Error renaming %s to %s: %v", oldPath, newPath, err)
		return errnoFromBackendError(backendOpRename, err)
	}

	actualOldPath := wsInfo.Path
	actualNewPath := renameTargetPath(wsInfo, newPath)
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		if err := n.diskCache.Delete(actualOldPath); err != nil {
			logging.Debugf("Failed to delete old path from cache %s: %v", actualOldPath, err)
		}
		if err := n.diskCache.Delete(actualNewPath); err != nil {
			logging.Debugf("Failed to delete new path from cache %s: %v", actualNewPath, err)
		}
	}

	if childInode != nil {
		if languageChanged {
			refreshRenamedNode(opCtx, n.wfClient, childInode, newPath, actualNewPath)
			notifyContentIfPossible(childInode, newPath)
		}
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
