package main

import (
	"context"
	"encoding/base64"
	"log"
	"path"
	"syscall"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type WSNode struct {
	fs.Inode
	wfclient   *WorkspaceFilesClient
	path       string
	objectType workspace.ObjectType
	size       int64
	data       []byte
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))

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

	listReq := NewListFilesRequest(n.path)
	objects, err := n.wfclient.ListFiles(ctx, listReq)
	if err != nil {
		log.Printf("Error listing workspace: %v", err)
		return nil, syscall.EIO
	}

	for _, obj := range objects.Objects {
		name := path.Base(obj.ObjectInfo.Path)
		mode := uint32(syscall.S_IFREG)
		if obj.ObjectInfo.ObjectType == "DIRECTORY" {
			mode = uint32(syscall.S_IFDIR)
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

	// Check cache first
	if info, ok := getCachedObjectInfo(fullPath); ok {
		if info == nil {
			return nil, syscall.ENOENT
		}

		child := n.NewPersistentInode(ctx, &WSNode{
			wfclient:   n.wfclient,
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

	info, err := n.wfclient.workspaceClient.Workspace.GetStatusByPath(ctx, fullPath)
	if err != nil {
		setCachedObjectInfo(fullPath, nil)
		return nil, syscall.ENOENT
	}
	setCachedObjectInfo(fullPath, info)

	child := n.NewPersistentInode(ctx, &WSNode{
		wfclient:   n.wfclient,
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
		resp, err := n.wfclient.workspaceClient.Workspace.Export(ctx, workspace.ExportRequest{
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
