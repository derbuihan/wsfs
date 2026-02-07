package fuse

import (
	"context"
	"syscall"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fs"

	"wsfs/internal/databricks"
)

func TestRenameUpdatesDescendantPaths(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			return nil
		},
	}

	root := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/",
		}},
	}

	fs.NewNodeFS(root, &fs.Options{})
	ctx := context.Background()

	dirNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/dir",
		}},
	}
	dirInode := root.NewPersistentInode(ctx, dirNode, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno(dirNode.fileInfo)})
	root.AddChild("dir", dirInode, false)

	subNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/dir/sub",
		}},
	}
	subInode := dirNode.NewPersistentInode(ctx, subNode, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno(subNode.fileInfo)})
	dirNode.AddChild("sub", subInode, false)

	fileNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/dir/sub/file.txt",
		}},
	}
	fileInode := subNode.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	subNode.AddChild("file.txt", fileInode, false)

	if errno := root.Rename(ctx, "dir", root, "renamed", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}

	if got := dirNode.fileInfo.Path; got != "/renamed" {
		t.Fatalf("Expected dir path '/renamed', got %q", got)
	}
	if got := subNode.fileInfo.Path; got != "/renamed/sub" {
		t.Fatalf("Expected subdir path '/renamed/sub', got %q", got)
	}
	if got := fileNode.fileInfo.Path; got != "/renamed/sub/file.txt" {
		t.Fatalf("Expected file path '/renamed/sub/file.txt', got %q", got)
	}
}
