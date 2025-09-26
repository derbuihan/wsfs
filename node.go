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
	wfClient *WorkspaceFilesClient
	objInfo  workspace.ObjectInfo
	data     []byte
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))

func (n *WSNode) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	switch n.objInfo.ObjectType {
	case workspace.ObjectTypeDirectory:
		out.Mode = 0755 | syscall.S_IFDIR
	default:
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(n.objInfo.Size)
		out.Atime = uint64(n.objInfo.ModifiedAt)
		out.Ctime = uint64(n.objInfo.ModifiedAt)
		out.Mtime = uint64(n.objInfo.ModifiedAt)
	}
	return 0
}

func (n *WSNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Printf("Readdir called on path: %s", n.objInfo.Path)

	if n.objInfo.ObjectType != workspace.ObjectTypeDirectory && n.objInfo.Path != "/" {
		return nil, syscall.ENOTDIR
	}

	entries := []fuse.DirEntry{}

	listReq := NewListFilesRequest(n.objInfo.Path)
	objects, err := n.wfClient.ListFiles(ctx, listReq)
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
	log.Printf("Lookup called on path: %s/%s", n.objInfo.Path, name)
	if n.objInfo.ObjectType != workspace.ObjectTypeDirectory && n.objInfo.Path != "/" {
		return nil, syscall.ENOTDIR
	}

	fullPath := path.Join(n.objInfo.Path, name)
	if n.objInfo.Path == "/" {
		fullPath = "/" + name
	}

	infoReq := NewObjectInfoRequest(fullPath)
	info, err := n.wfClient.ObjectInfo(ctx, infoReq)
	if err != nil {
		setCachedObjectInfo(fullPath, nil)
		return nil, syscall.ENOENT
	}

	objectInfo := info.WsfsObjectInfo.ObjectInfo

	child := n.NewPersistentInode(ctx, &WSNode{
		wfClient: n.wfClient,
		objInfo:  objectInfo,
	}, fs.StableAttr{Mode: getMode(objectInfo.ObjectType)})

	if objectInfo.ObjectType == workspace.ObjectTypeDirectory {
		out.Mode = 0755 | syscall.S_IFDIR
	} else {
		out.Mode = 0644 | syscall.S_IFREG
		out.Size = uint64(objectInfo.Size)
	}

	return child, 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	log.Printf("Open called on path: %s", n.objInfo.Path)

	if n.objInfo.ObjectType == workspace.ObjectTypeDirectory {
		return nil, 0, syscall.EISDIR
	}

	if n.data == nil {
		resp, err := n.wfClient.workspaceClient.Workspace.Export(ctx, workspace.ExportRequest{
			Path:   n.objInfo.Path,
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
		// n.size = int64(len(n.data))
	}

	return nil, fuse.FOPEN_KEEP_CACHE, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	log.Printf("Read called on path: %s, offset: %d, size: %d", n.objInfo.Path, off, len(dest))

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
