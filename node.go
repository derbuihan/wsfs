package main

import (
	"context"
	"log"
	"path"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type WSNode struct {
	fs.Inode
	wfClient *WorkspaceFilesClient
	fileInfo WSFileInfo
	data     []byte
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))
var _ = (fs.NodeWriter)((*WSNode)(nil))
var _ = (fs.NodeFlusher)((*WSNode)(nil))
var _ = (fs.NodeCreater)((*WSNode)(nil))
var _ = (fs.NodeUnlinker)((*WSNode)(nil))
var _ = (fs.NodeMkdirer)((*WSNode)(nil))
var _ = (fs.NodeRmdirer)((*WSNode)(nil))

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("Getattr called on path: %s", n.fileInfo.Path)

	wsInfo := n.fileInfo
	// Set the attributes for the file or directory
	if wsInfo.IsDir() {
		out.Mode = syscall.S_IFDIR | 0755
		out.Nlink = 2
		out.Size = 4096
	} else {
		out.Mode = syscall.S_IFREG | 0644
		out.Nlink = 1
		out.Size = uint64(wsInfo.Size())
	}

	// Block size
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

	out.SetTimeout(60)

	return 0
}

func (n *WSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("Readdir called on path: %s", n.fileInfo.Path)

	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	entries, err := n.wfClient.ReadDir(ctx, n.fileInfo.Path)
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
	log.Printf("Lookup called on path: %s/%s", n.fileInfo.Path, name)
	if !n.fileInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	childPath := path.Join(n.fileInfo.Path, name)

	info, err := n.wfClient.Stat(ctx, childPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	wsInfo := info.(WSFileInfo)

	// Set the attributes for the file or directory
	if wsInfo.IsDir() {
		out.Mode = syscall.S_IFDIR | 0755
		out.Nlink = 2
		out.Size = 4096
	} else {
		out.Mode = syscall.S_IFREG | 0644
		out.Nlink = 1
		out.Size = uint64(wsInfo.Size())
	}

	// Block size
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

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo}
	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})
	return child, 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("Open called on path: %s", n.fileInfo.Path)
	if n.fileInfo.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	if n.data == nil {
		data, err := n.wfClient.ReadAll(ctx, n.fileInfo.Path)
		if err != nil {
			return nil, 0, syscall.EIO
		}
		n.data = data
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("Read called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(dest))

	if n.data == nil {
		log.Printf("Data is nil, file might not be opened properly")
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
	log.Printf("Write called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(data))

	if n.data == nil {
		log.Printf("Error: node data is nil on Write for %s", n.fileInfo.Path)
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
	log.Printf("Flash called on path: %s", n.fileInfo.Path)

	if n.data == nil {
		return 0
	}

	err := n.wfClient.Write(ctx, n.fileInfo.Path, n.data)
	if err != nil {
		log.Printf("Error writting back on Flush: %v", err)
		return syscall.EIO
	}

	return 0
}

func (n *WSNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("Create called in dir: %s, for file: %s", n.fileInfo.Path, name)

	childPath := path.Join(n.fileInfo.Path, name)

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

	// Set the attributes for the file
	out.Mode = syscall.S_IFREG | 0644
	out.Nlink = 1
	out.Size = uint64(wsInfo.Size())

	// Block size
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

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo, data: []byte{}}
	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})
	return child, nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Unlink(ctx context.Context, name string) syscall.Errno {
	log.Printf("Unlink called in dir: %s, for file: %s", n.fileInfo.Path, name)

	childPath := path.Join(n.fileInfo.Path, name)

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
	log.Printf("Mkdir called in dir: %s, for new dir: %s", n.fileInfo.Path, name)

	childPath := path.Join(n.fileInfo.Path, name)
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

	// Set the attributes for the file or directory
	out.Mode = syscall.S_IFDIR | 0755
	out.Nlink = 2
	out.Size = 4096

	// Block size
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

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	childNode := &WSNode{wfClient: n.wfClient, fileInfo: wsInfo}
	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: uint32(out.Mode)})

	return child, 0
}

func (n *WSNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	log.Printf("Rmdir called in dir: %s, for dir: %s", n.fileInfo.Path, name)

	childPath := path.Join(n.fileInfo.Path, name)

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
