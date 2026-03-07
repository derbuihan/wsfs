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

func testNotebookInfo(path string, language workspace.Language) databricks.WSFileInfo {
	return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
		ObjectType: workspace.ObjectTypeNotebook,
		Path:       path,
		Language:   language,
		Size:       0,
	}}
}

func readNodeText(t *testing.T, node *WSNode) string {
	t.Helper()

	result, errno := node.Read(context.Background(), nil, make([]byte, 4096), 0)
	if errno != 0 {
		t.Fatalf("Read failed with errno: %d", errno)
	}
	data, status := result.Bytes(nil)
	if status != 0 {
		t.Fatalf("Read bytes failed with status: %d", status)
	}
	return string(data)
}

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

func TestRenameNotebookLanguageChangeFlushesDirtyBuffer(t *testing.T) {
	const (
		sourcePath    = "/dir/file"
		sourceVisible = "/dir/file.py"
		destVisible   = "/dir/file.sql"
		dirtySource   = "# Databricks notebook source\nprint(123)\n# COMMAND ----------\nprint(456)\n"
		sqlSource     = "-- Databricks notebook source\nprint(123)\n-- COMMAND ----------\nprint(456)\n"
	)

	renamed := false
	writeCalled := false
	remoteContents := map[string][]byte{
		sourcePath: []byte("stale remote content\n"),
	}

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalled = true
			if filepath != sourcePath {
				t.Fatalf("unexpected write path: %s", filepath)
			}
			remoteContents[filepath] = append([]byte(nil), data...)
			return nil
		},
		ReadAllFunc: func(ctx context.Context, filepath string) ([]byte, error) {
			data, ok := remoteContents[filepath]
			if !ok {
				return nil, iofs.ErrNotExist
			}
			return append([]byte(nil), data...), nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case sourceVisible:
				if renamed {
					return nil, iofs.ErrNotExist
				}
				return testNotebookInfo(sourcePath, workspace.LanguagePython), nil
			case sourcePath, destVisible:
				language := workspace.LanguagePython
				if renamed {
					language = workspace.LanguageSql
				}
				return testNotebookInfo(sourcePath, language), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			if sourcePath != sourceVisible || destinationPath != destVisible {
				t.Fatalf("unexpected rename: %s -> %s", sourcePath, destinationPath)
			}
			if got := string(remoteContents["/dir/file"]); got != dirtySource {
				t.Fatalf("expected rename to see flushed content, got %q", got)
			}
			renamed = true
			remoteContents["/dir/file"] = []byte(sqlSource)
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
		fileInfo: testNotebookInfo(sourcePath, workspace.LanguagePython),
		buf: fileBuffer{
			Data: []byte(dirtySource),
		},
		openCount: 1,
	}
	fileNode.markDirtyLocked(dirtyData)
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.py", fileInode, false)

	if errno := root.Rename(ctx, "file.py", root, "file.sql", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}
	if !writeCalled {
		t.Fatal("expected dirty buffer to be flushed before rename")
	}

	if got := fileNode.fileInfo.Language; got != workspace.LanguageSql {
		t.Fatalf("Expected SQL language after rename, got %q", got)
	}
	if fileNode.isDirtyLocked() {
		t.Fatal("expected node to be clean after refresh")
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
	if got := readNodeText(t, fileNode); got != sqlSource {
		t.Fatalf("unexpected content after rename: %q", got)
	}
}

func TestRenameNotebookCrossBasenameLanguageChangeRefreshesNode(t *testing.T) {
	const (
		sourcePath    = "/dir/file"
		destPath      = "/dir/renamed"
		sourceVisible = "/dir/file.py"
		destVisible   = "/dir/renamed.sql"
		pythonSource  = "# Databricks notebook source\nprint(123)\n# COMMAND ----------\nprint(456)\n"
		sqlSource     = "-- Databricks notebook source\nprint(123)\n-- COMMAND ----------\nprint(456)\n"
	)

	renamed := false
	remoteContents := map[string][]byte{
		sourcePath: []byte(pythonSource),
	}

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filepath string) ([]byte, error) {
			data, ok := remoteContents[filepath]
			if !ok {
				return nil, iofs.ErrNotExist
			}
			return append([]byte(nil), data...), nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case sourceVisible:
				if renamed {
					return nil, iofs.ErrNotExist
				}
				return testNotebookInfo(sourcePath, workspace.LanguagePython), nil
			case sourcePath:
				if renamed {
					return nil, iofs.ErrNotExist
				}
				return testNotebookInfo(sourcePath, workspace.LanguagePython), nil
			case destVisible, destPath:
				if !renamed {
					return nil, iofs.ErrNotExist
				}
				return testNotebookInfo(destPath, workspace.LanguageSql), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePathArg string, destinationPath string) error {
			if sourcePathArg != sourceVisible || destinationPath != destVisible {
				t.Fatalf("unexpected rename: %s -> %s", sourcePathArg, destinationPath)
			}
			renamed = true
			delete(remoteContents, sourcePath)
			remoteContents[destPath] = []byte(sqlSource)
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
		fileInfo: testNotebookInfo(sourcePath, workspace.LanguagePython),
		buf: fileBuffer{
			Data: []byte(pythonSource),
		},
	}
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.py", fileInode, false)

	if errno := root.Rename(ctx, "file.py", root, "renamed.sql", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}

	if got := fileNode.fileInfo.Path; got != destPath {
		t.Fatalf("expected path %q after rename, got %q", destPath, got)
	}
	if got := fileNode.fileInfo.Language; got != workspace.LanguageSql {
		t.Fatalf("expected SQL language after rename, got %q", got)
	}
	if fileNode.buf.Data != nil {
		t.Fatalf("expected cached data cleared after rename, got %q", string(fileNode.buf.Data))
	}
	if fileNode.isDirtyLocked() {
		t.Fatal("expected node to be clean after refresh")
	}
	if got := readNodeText(t, fileNode); got != sqlSource {
		t.Fatalf("unexpected content after rename: %q", got)
	}
}

func TestRenameNotebookLanguageChangeFlushFailureStopsRename(t *testing.T) {
	renameCalled := false

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return iofs.ErrPermission
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case "/dir/file.py", "/dir/file":
				return testNotebookInfo("/dir/file", workspace.LanguagePython), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			renameCalled = true
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
		wfClient:  api,
		fileInfo:  testNotebookInfo("/dir/file", workspace.LanguagePython),
		buf:       fileBuffer{Data: []byte("# Databricks notebook source\nprint(123)\n")},
		openCount: 1,
	}
	fileNode.markDirtyLocked(dirtyData)
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.py", fileInode, false)

	if errno := root.Rename(ctx, "file.py", root, "file.sql", 0); errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
	if renameCalled {
		t.Fatal("expected rename to stop before remote rename call")
	}
	if !fileNode.isDirtyLocked() {
		t.Fatal("expected node to remain dirty after flush failure")
	}
}
