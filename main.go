package main

import (
	"context"
	"encoding/base64"
	"flag"
	"log"
	"os"
	"path"
	"syscall"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/workspace"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type WSNode struct {
	fs.Inode
	client     *databricks.WorkspaceClient
	path       string
	objectType workspace.ObjectType
	size       int64
	data       []byte
}

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if n.objectType == workspace.ObjectTypeDirectory {
		out.Mode = 0755 | syscall.S_IFDIR
	} else {
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(n.size)
	}
	return 0
}

func (n *WSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("Readdir called on path: %s", n.path)

	if n.objectType != workspace.ObjectTypeDirectory && n.path != "/" {
		return nil, syscall.ENOTDIR
	}

	entries := []fuse.DirEntry{}

	listReq := workspace.ListWorkspaceRequest{
		Path: n.path,
	}

	objects, err := n.client.Workspace.ListAll(ctx, listReq)
	if err != nil {
		log.Printf("Error listing workspace: %v", err)
		return nil, syscall.EIO
	}

	for _, obj := range objects {
		name := path.Base(obj.Path)
		mode := uint32(syscall.S_IFREG)
		if obj.ObjectType == workspace.ObjectTypeDirectory {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Name: name, Mode: mode})
	}

	return fs.NewListDirStream(entries), 0
}

func getMode(objectType workspace.ObjectType) uint32 {
	if objectType == workspace.ObjectTypeDirectory {
		return syscall.S_IFDIR | 0755
	}
	return syscall.S_IFREG | 0644
}

func (n *WSNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Printf("Lookup called on path: %s/%s", n.path, name)
	if n.objectType != workspace.ObjectTypeDirectory && n.path != "/" {
		return nil, syscall.ENOTDIR
	}

	fullPath := path.Join(n.path, name)
	if n.path == "/" {
		fullPath = "/" + name
	}

	info, err := n.client.Workspace.GetStatusByPath(ctx, fullPath)
	if err != nil {
		return nil, syscall.ENOENT
	}

	child := n.NewPersistentInode(ctx, &WSNode{
		client:     n.client,
		path:       fullPath,
		objectType: info.ObjectType,
		size:       info.Size,
	}, fs.StableAttr{Mode: getMode(info.ObjectType)})

	if info.ObjectType == workspace.ObjectTypeDirectory {
		out.Mode = 0755 | syscall.S_IFDIR
	} else {
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(info.Size)
	}

	return child, 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("Open called on path: %s", n.path)

	if n.objectType == workspace.ObjectTypeDirectory {
		return nil, 0, syscall.EISDIR
	}

	if n.data == nil {
		resp, err := n.client.Workspace.Export(ctx, workspace.ExportRequest{
			Path:   n.path,
			Format: workspace.ExportFormatSource,
		})
		if err != nil {
			log.Printf("Error exporting file: %v", err)
			return nil, 0, syscall.EIO
		}

		dec, err := base64.StdEncoding.DecodeString(resp.Content)
		if err != nil {
			log.Printf("Error decoding base64 content: %v", err)
			return nil, 0, syscall.EIO
		}

		n.data = []byte(dec)
		n.size = int64(len(n.data))
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("Read called on path: %s, offset: %d, size: %d", n.path, off, len(dest))

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

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatalf("Usage: %s MOUNTPOINT", os.Args[0])
	}

	client := databricks.Must(databricks.NewWorkspaceClient())

	root := &WSNode{
		client:     client,
		path:       "/",
		objectType: workspace.ObjectTypeDirectory,
	}

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			AllowOther: true,
			Name:       "wsfs",
			FsName:     "wsfs",
		},
	}
	opts.Debug = *debug

	server, err := fs.Mount(flag.Arg(0), root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	log.Printf("Mounted Databricks workspace on %s", flag.Arg(0))
	log.Println("Press Ctrl+C to unmount")

	server.Wait()
}
