package main

import (
	"context"
	"log"
	"path"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type WSNode struct {
	fs.Inode
	wfClient *WorkspaceFilesClient
	fileInfo WSFileInfo
	data     []byte
	mu       sync.Mutex
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeSetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))
var _ = (fs.NodeWriter)((*WSNode)(nil))
var _ = (fs.NodeFlusher)((*WSNode)(nil))
var _ = (fs.NodeFsyncer)((*WSNode)(nil))
var _ = (fs.NodeCreater)((*WSNode)(nil))
var _ = (fs.NodeUnlinker)((*WSNode)(nil))
var _ = (fs.NodeMkdirer)((*WSNode)(nil))
var _ = (fs.NodeRmdirer)((*WSNode)(nil))
var _ = (fs.NodeRenamer)((*WSNode)(nil))

func (n *WSNode) Path() string {
	return n.fileInfo.Path
}

func (n *WSNode) fillAttr(ctx context.Context, out *fuse.Attr) {
	wsInfo := n.fileInfo

	// Set the attributes for the file or directory
	if wsInfo.IsDir() {
		out.Mode = syscall.S_IFDIR | 0755
		out.Nlink = 2
	} else {
		out.Mode = syscall.S_IFREG | 0644
		out.Nlink = 1
	}

	// Block size
	out.Size = uint64(wsInfo.Size())
	out.Blksize = 4096
	out.Blocks = (out.Size + 511) / 512

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
	debugf("Getattr called on path: %s", n.Path())

	n.fillAttr(ctx, &out.Attr)
	out.SetTimeout(60)

	return 0
}

func (n *WSNode) Setattr(ctx context.Context, fh fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	debugf("Setattr called on path: %s", n.Path())

	if _, ok := in.GetMTime(); ok {
		debugf("Setattr called on path %s to change mtime (operation ignored)", n.Path())
	}
	n.fillAttr(ctx, &out.Attr)

	return 0
}

func (n *WSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	debugf("Readdir called on path: %s", n.Path())

	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	entries, err := n.wfClient.ReadDir(ctx, n.Path())
	if err != nil {
		return nil, syscall.EIO
	}

	fuseEntries := make([]fuse.DirEntry, len(entries))
	for i, e := range entries {
		mode := uint32(syscall.S_IFREG)
		if e.IsDir() {
			mode = uint32(syscall.S_IFDIR)
		}
		fuseEntries[i] = fuse.DirEntry{Name: e.Name(), Mode: mode}
	}

	return fs.NewListDirStream(fuseEntries), 0
}

func (n *WSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	debugf("Lookup called on path: %s/%s", n.Path(), name)
	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	childPath := path.Join(n.Path(), name)

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	wsInfo := info.(WSFileInfo)

	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo}
	childNode.fillAttr(ctx, &out.Attr)

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})
	return child, 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	debugf("Open called on path: %s", n.Path())

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.fileInfo.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	if n.data == nil {
		data, err := n.wfClient.ReadAll(ctx, n.Path())
		if err != nil {
			return nil, 0, syscall.EIO
		}
		n.data = data
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	debugf("Read called on path: %s, offset: %d, size: %d", n.Path(), off, len(dest))

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.data == nil {
		debugf("Data is nil, file might not be opened properly")
		return nil, syscall.EIO
	}

	end := off + int64(len(dest))
	if end > int64(len(n.data)) {
		end = int64(len(n.data))
	}

	if off >= int64(len(n.data)) {
		return fuse.ReadResultData([]byte{}), 0
	}

	result := n.data[off:end]
	return fuse.ReadResultData(result), 0
}

func (n *WSNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	debugf("Write called on path: %s, offset: %d, size: %d", n.Path(), off, len(data))

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.data == nil {
		log.Printf("Error: node data is nil on Write for %s", n.Path())
		return 0, syscall.EIO
	}

	end := off + int64(len(data))
	if int64(len(n.data)) < end {
		newData := make([]byte, end)
		copy(newData, n.data)
		n.data = newData
	}
	copy(n.data[off:], data)

	n.fileInfo.ObjectInfo.Size = int64(len(n.data))

	return uint32(len(data)), 0
}

func (n *WSNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	debugf("Flush called on path: %s", n.Path())

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.data == nil {
		return 0
	}

	err := n.wfClient.Write(ctx, n.Path(), n.data)
	if err != nil {
		log.Printf("Error writting back on Flush: %v", err)
		return syscall.EIO
	}

	info, err := n.wfClient.Stat(ctx, n.Path())
	if err != nil {
		log.Printf("Error refreshing file info after Flush: %v", err)
	}
	n.fileInfo = info.(WSFileInfo)

	return 0
}

func (n *WSNode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	debugf("Fsync called on path: %s", n.Path())

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.data == nil {
		return 0
	}

	err := n.wfClient.Write(ctx, n.Path(), n.data)
	if err != nil {
		log.Printf("Error writting back on Flush: %v", err)
		return syscall.EIO
	}

	info, err := n.wfClient.Stat(ctx, n.Path())
	if err != nil {
		log.Printf("Error refreshing file info after Flush: %v", err)
	}
	n.fileInfo = info.(WSFileInfo)

	return 0
}

func (n *WSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	debugf("Create called in dir: %s, for file: %s", n.Path(), name)

	childPath := path.Join(n.Path(), name)

	err := n.wfClient.Write(ctx, childPath, []byte{})
	if err != nil {
		log.Printf("Error creating file on databricks: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		log.Printf("Error stating new file: %v", err)
		return nil, nil, 0, syscall.EIO
	}

	wsInfo := info.(WSFileInfo)
	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo, data: []byte{}}
	childNode.fillAttr(ctx, &out.Attr)

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})
	return child, nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	debugf("Unlink called in dir: %s, for file: %s", n.Path(), name)

	childPath := path.Join(n.Path(), name)

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		return syscall.ENOENT
	}

	if info.IsDir() {
		return syscall.EISDIR
	}

	err = n.wfClient.Delete(ctx, childPath, false)
	if err != nil {
		log.Printf("Error deleting file on databricks: %v", err)
		return syscall.EIO
	}

	return 0
}

func (n *WSNode) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	debugf("Mkdir called in dir: %s, for new dir: %s", n.Path(), name)

	childPath := path.Join(n.Path(), name)
	err := n.wfClient.Mkdir(ctx, childPath)
	if err != nil {
		log.Printf("Error creating directory on databricks: %v", err)
		return nil, syscall.EIO
	}

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		log.Printf("Error stating new directory: %v", err)
		return nil, syscall.EIO
	}

	wsInfo := info.(WSFileInfo)
	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo}
	childNode.fillAttr(ctx, &out.Attr)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})
	return child, 0
}

func (n *WSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	debugf("Rmdir called in dir: %s, for dir: %s", n.Path(), name)

	childPath := path.Join(n.Path(), name)

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		return syscall.ENOENT
	}
	if !info.IsDir() {
		return syscall.ENOTDIR
	}

	err = n.wfClient.Delete(ctx, childPath, false)
	if err != nil {
		log.Printf("Error deleting directory on databricks: %v", err)
		return syscall.EIO
	}

	return 0
}

func (n *WSNode) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	debugf("Rename called from %s to %s", name, newName)

	newParentNode, ok := newParent.EmbeddedInode().Operations().(*WSNode)
	if !ok {
		return syscall.EIO
	}

	oldPath := path.Join(n.Path(), name)
	newPath := path.Join(newParentNode.fileInfo.Path, newName)

	err := n.wfClient.Rename(ctx, oldPath, newPath)
	if err != nil {
		return syscall.EIO
	}

	childInode := n.GetChild(name)
	if childInode != nil {
		childNode, ok := childInode.Operations().(*WSNode)
		if ok {
			debugf("Updating internal path for in-memory node from '%s' to '%s'", childNode.fileInfo.Path, newPath)
			childNode.fileInfo.Path = newPath
		}
	}

	return 0
}

func NewRootNode(wfClient *WorkspaceFilesClient, rootPath string) (*WSNode, error) {
	info, err := wfClient.Stat(context.Background(), rootPath)

	if err != nil {
		return nil, err
	}

	wsInfo := info.(WSFileInfo)
	if !wsInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	return &WSNode{
		wfClient: wfClient,
		fileInfo: wsInfo,
	}, nil
}
