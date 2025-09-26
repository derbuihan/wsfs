package main

import (
	"context"
	"log"
	"path"
	"syscall"

	"github.com/databricks/databricks-sdk-go/service/workspace"
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

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	log.Printf("Getattr called on path: %s", n.fileInfo.Path)
	switch n.fileInfo.ObjectType {
	case workspace.ObjectTypeDirectory:
		out.Mode = 0755 | syscall.S_IFDIR
	default:
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(n.fileInfo.Size())
	}

	modTime := n.fileInfo.ModTime()
	out.Mtime = uint64(modTime.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime

	out.SetTimeout(60)

	return 0
}

func (entry WSDirEntry) ToFuseDirEntry() fuse.DirEntry {
	mode := uint32(syscall.S_IFREG | 0644)
	if entry.IsDir() {
		mode = uint32(syscall.S_IFDIR | 0755)
	}

	return fuse.DirEntry{
		Name: entry.Name(),
		Mode: mode,
	}
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

func getMode(objectType workspace.ObjectType) uint32 {
	if objectType == workspace.ObjectTypeDirectory {
		return syscall.S_IFDIR | 0755
	}
	return syscall.S_IFREG | 0644
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

	childNode := &WSNode{
		wfClient: n.wfClient,
		fileInfo: wsInfo,
	}

	if wsInfo.IsDir() {
		out.Mode = syscall.S_IFDIR | 0755
	} else {
		out.Mode = syscall.S_IFREG | 0644
		out.Size = uint64(wsInfo.Size())
	}

	modTime := wsInfo.ModTime()
	out.Mtime = uint64(modTime.Unix())
	out.Atime = out.Mtime
	out.Ctime = out.Mtime

	out.SetEntryTimeout(60)
	out.SetAttrTimeout(60)

	child := n.NewPersistentInode(ctx, childNode, fs.StableAttr{
		Mode: uint32(out.Mode),
	})

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
