package fuse

import (
	"context"
	iofs "io/fs"
	"syscall"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fs"

	"wsfs/internal/databricks"
)

func TestRenameUpdatesDescendantPaths(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case "/dir":
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					ObjectType: workspace.ObjectTypeDirectory,
					Path:       "/dir",
				}}, nil
			default:
				return databricks.NewTestFileInfo(filePath, 0, false), nil
			}
		},
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

func TestRenameNotebookLanguageChangeRefreshesNode(t *testing.T) {
	renamed := false
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case "/":
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					ObjectType: workspace.ObjectTypeDirectory,
					Path:       "/",
				}}, nil
			case "/dir/file.py":
				if renamed {
					return nil, iofs.ErrNotExist
				}
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					ObjectType: workspace.ObjectTypeNotebook,
					Path:       "/dir/file",
					Language:   workspace.LanguagePython,
				}}, nil
			case "/dir/file":
				language := workspace.LanguagePython
				if renamed {
					language = workspace.LanguageSql
				}
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					ObjectType: workspace.ObjectTypeNotebook,
					Path:       "/dir/file",
					Language:   language,
				}}, nil
			case "/dir/file.sql":
				if !renamed {
					return nil, iofs.ErrNotExist
				}
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					ObjectType: workspace.ObjectTypeNotebook,
					Path:       "/dir/file",
					Language:   workspace.LanguageSql,
				}}, nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			renamed = true
			return nil
		},
	}

	root := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/dir",
		}},
	}

	fs.NewNodeFS(root, &fs.Options{})
	ctx := context.Background()

	fileNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeNotebook,
			Path:       "/dir/file",
			Language:   workspace.LanguagePython,
		}},
		buf: fileBuffer{
			Data:                []byte("# Databricks notebook source\nprint(123)\n"),
			ReplaceOnFirstWrite: true,
		},
	}
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.py", fileInode, false)

	if errno := root.Rename(ctx, "file.py", root, "file.sql", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}

	if got := fileNode.fileInfo.Language; got != workspace.LanguageSql {
		t.Fatalf("Expected SQL language after rename, got %q", got)
	}
	if fileNode.buf.Data != nil {
		t.Fatalf("Expected buffer invalidated after rename, got %q", string(fileNode.buf.Data))
	}
	if fileNode.buf.CachedPath != "" {
		t.Fatalf("Expected cache path cleared, got %q", fileNode.buf.CachedPath)
	}
	if fileNode.buf.ReplaceOnFirstWrite {
		t.Fatal("Expected ReplaceOnFirstWrite to be cleared")
	}
}
