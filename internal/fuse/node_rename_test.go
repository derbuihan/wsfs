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

	if errno := root.Rename(ctx, "file.py", root, "file.sql", 0); errno != syscall.EACCES {
		t.Fatalf("expected EACCES, got %d", errno)
	}
	if renameCalled {
		t.Fatal("expected rename to stop before remote rename call")
	}
	if !fileNode.isDirtyLocked() {
		t.Fatal("expected node to remain dirty after flush failure")
	}
}

func TestRenameRegularFileFlushesDirtyBuffer(t *testing.T) {
	const (
		sourcePath  = "/dir/file.txt"
		destPath    = "/dir/renamed.txt"
		dirtySource = "dirty regular file contents\n"
	)

	renameCalled := false
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
			case sourcePath:
				if renameCalled {
					return nil, iofs.ErrNotExist
				}
				return databricks.NewTestFileInfo(sourcePath, int64(len(dirtySource)), false), nil
			case destPath:
				if !renameCalled {
					return nil, iofs.ErrNotExist
				}
				return databricks.NewTestFileInfo(destPath, int64(len(dirtySource)), false), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePathArg string, destinationPath string) error {
			if sourcePathArg != sourcePath || destinationPath != destPath {
				t.Fatalf("unexpected rename: %s -> %s", sourcePathArg, destinationPath)
			}
			if got := string(remoteContents[sourcePath]); got != dirtySource {
				t.Fatalf("expected flushed content before rename, got %q", got)
			}
			renameCalled = true
			remoteContents[destPath] = append([]byte(nil), remoteContents[sourcePath]...)
			delete(remoteContents, sourcePath)
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
			ObjectType: workspace.ObjectTypeFile,
			Path:       sourcePath,
			Size:       int64(len(dirtySource)),
		}},
		buf: fileBuffer{
			Data:           []byte(dirtySource),
			CachedPath:     "/tmp/stale-cache",
			CachedChecksum: "deadbeef",
		},
		openCount: 1,
	}
	fileNode.markDirtyLocked(dirtyData)
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.txt", fileInode, false)

	if errno := root.Rename(ctx, "file.txt", root, "renamed.txt", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}
	if !writeCalled {
		t.Fatal("expected dirty regular file to flush before rename")
	}
	if got := fileNode.fileInfo.Path; got != destPath {
		t.Fatalf("expected path %q after rename, got %q", destPath, got)
	}
	if fileNode.isDirtyLocked() {
		t.Fatal("expected node to be clean after rename refresh")
	}
	if fileNode.buf.Data != nil {
		t.Fatalf("expected clean buffer invalidated after rename, got %q", string(fileNode.buf.Data))
	}
	if fileNode.buf.CachedPath != "" || fileNode.buf.CachedChecksum != "" {
		t.Fatalf("expected cached file metadata cleared, got path=%q checksum=%q", fileNode.buf.CachedPath, fileNode.buf.CachedChecksum)
	}
	if got := readNodeText(t, fileNode); got != dirtySource {
		t.Fatalf("unexpected content after rename: %q", got)
	}
}

func TestRenameRegularFileFlushFailureStopsRename(t *testing.T) {
	renameCalled := false

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return iofs.ErrPermission
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case "/dir/file.txt":
				return databricks.NewTestFileInfo("/dir/file.txt", 5, false), nil
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
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/dir/file.txt",
			Size:       5,
		}},
		buf:       fileBuffer{Data: []byte("dirty")},
		openCount: 1,
	}
	fileNode.markDirtyLocked(dirtyData)
	fileInode := root.NewPersistentInode(ctx, fileNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(fileNode.fileInfo)})
	root.AddChild("file.txt", fileInode, false)

	if errno := root.Rename(ctx, "file.txt", root, "renamed.txt", 0); errno != syscall.EACCES {
		t.Fatalf("expected EACCES, got %d", errno)
	}
	if renameCalled {
		t.Fatal("expected rename to stop before remote rename call")
	}
	if !fileNode.isDirtyLocked() {
		t.Fatal("expected node to remain dirty after flush failure")
	}
	if got := fileNode.fileInfo.Path; got != "/dir/file.txt" {
		t.Fatalf("expected source path to remain unchanged, got %q", got)
	}
}

func TestRenameRegularFileOverwriteInvalidatesDestinationNode(t *testing.T) {
	const (
		sourcePath = "/dir/index.lock"
		destPath   = "/dir/index"
		oldData    = "old index data\n"
		newData    = "new index data\n"
	)

	renamed := false
	remoteContents := map[string][]byte{
		sourcePath: []byte(newData),
		destPath:   []byte(oldData),
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
			case sourcePath:
				if renamed {
					return nil, iofs.ErrNotExist
				}
				return databricks.NewTestFileInfo(sourcePath, int64(len(newData)), false), nil
			case destPath:
				return databricks.NewTestFileInfo(destPath, int64(len(remoteContents[destPath])), false), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePathArg string, destinationPath string) error {
			if sourcePathArg != sourcePath || destinationPath != destPath {
				t.Fatalf("unexpected rename: %s -> %s", sourcePathArg, destinationPath)
			}
			renamed = true
			remoteContents[destPath] = append([]byte(nil), remoteContents[sourcePath]...)
			delete(remoteContents, sourcePath)
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

	sourceNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       sourcePath,
			Size:       int64(len(newData)),
		}},
	}
	sourceInode := root.NewPersistentInode(ctx, sourceNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(sourceNode.fileInfo)})
	root.AddChild("index.lock", sourceInode, false)

	destNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       destPath,
			Size:       int64(len(oldData)),
		}},
		buf: fileBuffer{Data: []byte(oldData)},
	}
	destInode := root.NewPersistentInode(ctx, destNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(destNode.fileInfo)})
	root.AddChild("index", destInode, false)

	if errno := root.Rename(ctx, "index.lock", root, "index", 0); errno != 0 {
		t.Fatalf("Rename failed with errno: %d", errno)
	}

	if destNode.buf.Data != nil {
		t.Fatalf("expected overwritten destination buffer cleared, got %q", string(destNode.buf.Data))
	}
	if destNode.buf.CachedPath != "" || destNode.buf.CachedChecksum != "" {
		t.Fatalf("expected overwritten destination cache metadata cleared, got path=%q checksum=%q", destNode.buf.CachedPath, destNode.buf.CachedChecksum)
	}
	if got := readNodeText(t, destNode); got != newData {
		t.Fatalf("expected overwritten destination to refetch new data, got %q", got)
	}
}

func TestRenameRegularFileOverwriteDirtyDestinationStopsRename(t *testing.T) {
	const (
		sourcePath = "/dir/index.lock"
		destPath   = "/dir/index"
	)

	renameCalled := false

	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			switch filePath {
			case sourcePath:
				return databricks.NewTestFileInfo(sourcePath, 4, false), nil
			case destPath:
				return databricks.NewTestFileInfo(destPath, 5, false), nil
			default:
				return nil, iofs.ErrNotExist
			}
		},
		RenameFunc: func(ctx context.Context, sourcePathArg string, destinationPath string) error {
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

	sourceNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       sourcePath,
			Size:       4,
		}},
	}
	sourceInode := root.NewPersistentInode(ctx, sourceNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(sourceNode.fileInfo)})
	root.AddChild("index.lock", sourceInode, false)

	destNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       destPath,
			Size:       5,
		}},
		buf:       fileBuffer{Data: []byte("dirty")},
		openCount: 1,
	}
	destNode.markDirtyLocked(dirtyData)
	destInode := root.NewPersistentInode(ctx, destNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(destNode.fileInfo)})
	root.AddChild("index", destInode, false)

	if errno := root.Rename(ctx, "index.lock", root, "index", 0); errno != syscall.EBUSY {
		t.Fatalf("expected EBUSY, got %d", errno)
	}
	if renameCalled {
		t.Fatal("expected rename to stop before remote rename call")
	}
	if !destNode.isDirtyLocked() {
		t.Fatal("expected destination node to remain dirty after aborted overwrite")
	}
	if got := sourceNode.fileInfo.Path; got != sourcePath {
		t.Fatalf("expected source path to remain unchanged, got %q", got)
	}
}
