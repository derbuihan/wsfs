package fuse

import (
	"context"
	iofs "io/fs"
	"path"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
)

type dirFirstLookupAPI struct {
	entries         map[string]databricks.WSFileInfo
	warmedDirs      map[string]map[string]databricks.WSFileInfo
	readDirCalls    int
	backendStatHits int
}

func (a *dirFirstLookupAPI) Stat(ctx context.Context, filePath string) (iofs.FileInfo, error) {
	parent := path.Dir(filePath)
	name := path.Base(filePath)
	if warmed, ok := a.warmedDirs[parent]; ok {
		if info, found := warmed[name]; found {
			return info, nil
		}
		return nil, iofs.ErrNotExist
	}

	a.backendStatHits++
	if info, ok := a.entries[filePath]; ok {
		return info, nil
	}
	return nil, iofs.ErrNotExist
}

func (a *dirFirstLookupAPI) StatFresh(ctx context.Context, filePath string) (iofs.FileInfo, error) {
	return a.Stat(ctx, filePath)
}

func (a *dirFirstLookupAPI) ReadDir(ctx context.Context, dirPath string) ([]iofs.DirEntry, error) {
	a.readDirCalls++
	if a.warmedDirs == nil {
		a.warmedDirs = make(map[string]map[string]databricks.WSFileInfo)
	}

	warmed := make(map[string]databricks.WSFileInfo)
	entries := []iofs.DirEntry{}
	for filePath, info := range a.entries {
		if path.Dir(filePath) != dirPath {
			continue
		}
		warmed[path.Base(filePath)] = info
		entries = append(entries, databricks.WSDirEntry{WSFileInfo: info})
	}
	a.warmedDirs[dirPath] = warmed
	return entries, nil
}

func (a *dirFirstLookupAPI) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	return nil, nil
}
func (a *dirFirstLookupAPI) Write(ctx context.Context, filepath string, data []byte) error {
	return nil
}
func (a *dirFirstLookupAPI) Delete(ctx context.Context, filePath string, recursive bool) error {
	return nil
}
func (a *dirFirstLookupAPI) Mkdir(ctx context.Context, dirPath string) error { return nil }
func (a *dirFirstLookupAPI) Rename(ctx context.Context, sourcePath string, destinationPath string) error {
	return nil
}
func (a *dirFirstLookupAPI) CacheSet(path string, info iofs.FileInfo) {}
func (a *dirFirstLookupAPI) CacheInvalidate(filePath string)          {}
func (a *dirFirstLookupAPI) MetadataTTL() time.Duration               { return time.Second }

func newTestRootNode(t *testing.T, api databricks.WorkspaceFilesAPI) *WSNode {
	t.Helper()
	root := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/",
		}},
	}
	fs.NewNodeFS(root, &fs.Options{})
	return root
}

func TestWSNodeLookupUsesDirtyChild(t *testing.T) {
	calls := 0
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			calls++
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}
	root := newTestRootNode(t, api)
	ctx := context.Background()

	childNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/file.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty"), Dirty: true},
	}
	childInode := root.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(childNode.fileInfo)})
	root.AddChild("file.txt", childInode, false)

	out := &fuse.EntryOut{}
	inode, errno := root.Lookup(ctx, "file.txt", out)
	if errno != 0 {
		t.Fatalf("Lookup errno: %d", errno)
	}
	if inode != childInode {
		t.Fatalf("expected existing inode")
	}
	if calls != 0 {
		t.Fatalf("expected no Stat calls, got %d", calls)
	}
	if out.Attr.Size != uint64(len(childNode.buf.Data)) {
		t.Fatalf("expected size %d, got %d", len(childNode.buf.Data), out.Attr.Size)
	}
}

func TestWSNodeLookupUsesCleanChildWithinTTL(t *testing.T) {
	calls := 0
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			calls++
			return databricks.NewTestFileInfo(filePath, 12, false), nil
		},
	}
	root := newTestRootNode(t, api)
	ctx := context.Background()

	childNode := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/file.txt",
			Size:       12,
			ModifiedAt: time.Now().UnixMilli(),
		}},
		metadataCheckedAt: time.Now(),
	}
	childInode := root.NewPersistentInode(ctx, childNode, fs.StableAttr{Mode: syscall.S_IFREG, Ino: stableIno(childNode.fileInfo)})
	root.AddChild("file.txt", childInode, false)

	out := &fuse.EntryOut{}
	inode, errno := root.Lookup(ctx, "file.txt", out)
	if errno != 0 {
		t.Fatalf("Lookup errno: %d", errno)
	}
	if inode != childInode {
		t.Fatalf("expected existing inode")
	}
	if calls != 0 {
		t.Fatalf("expected no Stat calls, got %d", calls)
	}
	if out.Attr.Size != 12 {
		t.Fatalf("expected size 12, got %d", out.Attr.Size)
	}
}

func TestWSNodeLookupErrors(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	inode, errno := root.Lookup(context.Background(), "missing", out)
	if errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %d", errno)
	}
	if inode != nil {
		t.Fatalf("expected nil inode on ENOENT")
	}
}

func TestWSNodeLookupInvalidName(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, errno := root.Lookup(context.Background(), "..", out)
	if errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}

func TestWSNodeLookupUnexpectedType(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return dummyFileInfo{}, nil
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, errno := root.Lookup(context.Background(), "file", out)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
}

func TestWSNodeLookupSuccess(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 10, false), nil
		},
	}
	root := newTestRootNode(t, api)
	root.ownerUid = 11
	root.ownerGid = 22
	root.restrictAccess = true

	out := &fuse.EntryOut{}
	inode, errno := root.Lookup(context.Background(), "file.txt", out)
	if errno != 0 || inode == nil {
		t.Fatalf("Lookup failed: errno=%d inode=%v", errno, inode)
	}
	child, ok := inode.Operations().(*WSNode)
	if !ok {
		t.Fatal("expected WSNode child")
	}
	if child.ownerUid != root.ownerUid || child.ownerGid != root.ownerGid || child.restrictAccess != root.restrictAccess {
		t.Fatal("child did not inherit access config")
	}
}

func TestWSNodeLookupWarmsParentReadDirBeforeStat(t *testing.T) {
	api := &dirFirstLookupAPI{
		entries: map[string]databricks.WSFileInfo{
			"/file1.txt": databricks.NewTestFileInfo("/file1.txt", 12, false),
			"/subdir":    databricks.NewTestFileInfo("/subdir", 0, true),
		},
	}
	root := newTestRootNode(t, api)

	if inode, errno := root.Lookup(context.Background(), "file1.txt", &fuse.EntryOut{}); errno != 0 || inode == nil {
		t.Fatalf("Lookup existing failed: errno=%d inode=%v", errno, inode)
	}
	if inode, errno := root.Lookup(context.Background(), "missing.txt", &fuse.EntryOut{}); errno != syscall.ENOENT || inode != nil {
		t.Fatalf("Lookup missing = (%v, %d), want (nil, ENOENT)", inode, errno)
	}

	if api.readDirCalls != 2 {
		t.Fatalf("expected 2 ReadDir warmups, got %d", api.readDirCalls)
	}
	if api.backendStatHits != 0 {
		t.Fatalf("expected 0 backend stat hits after ReadDir warmup, got %d", api.backendStatHits)
	}
}

func TestWSNodeOpendir(t *testing.T) {
	n := &WSNode{fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeFile}}}
	if errno := n.Opendir(context.Background()); errno != syscall.ENOTDIR {
		t.Fatalf("expected ENOTDIR, got %d", errno)
	}

	n = &WSNode{fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeDirectory}}}
	if errno := n.Opendir(context.Background()); errno != 0 {
		t.Fatalf("expected success, got %d", errno)
	}
}

func TestWSNodeOpendirHandle(t *testing.T) {
	n := &WSNode{fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeFile}}}
	if _, _, errno := n.OpendirHandle(context.Background(), 0); errno != syscall.ENOTDIR {
		t.Fatalf("expected ENOTDIR, got %d", errno)
	}

	n = &WSNode{fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeDirectory}}}
	h, flags, errno := n.OpendirHandle(context.Background(), 0)
	if errno != 0 || h == nil {
		t.Fatalf("expected handle, got errno=%d handle=%v", errno, h)
	}
	if flags != 0 {
		t.Fatalf("expected flags 0, got %d", flags)
	}
}

func TestWSNodeCreateFile(t *testing.T) {
	var wrotePath string
	var wroteData []byte
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			wrotePath = filepath
			wroteData = append([]byte(nil), data...)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, fh, flags, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}
	if fh == nil {
		t.Fatalf("expected non-nil file handle, got nil")
	}
	if flags != fuse.FOPEN_KEEP_CACHE {
		t.Fatalf("unexpected flags: %d", flags)
	}
	if wrotePath != "/file.txt" {
		t.Fatalf("unexpected write path: %s", wrotePath)
	}
	if len(wroteData) != 0 {
		t.Fatalf("expected empty file content, got %q", string(wroteData))
	}
}

func TestWSNodeCreateFileFirstWriteSkipsRemoteRead(t *testing.T) {
	readAllCalls := 0
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return []byte("unexpected"), nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}

	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}

	node, ok := child.Operations().(*WSNode)
	if !ok {
		t.Fatalf("expected WSNode child, got %T", child.Operations())
	}

	if _, errno := node.Write(context.Background(), nil, []byte("first write"), 0); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	}
	if readAllCalls != 0 {
		t.Fatalf("expected first write to skip remote ReadAll, got %d calls", readAllCalls)
	}
	if got := string(node.buf.Data); got != "first write" {
		t.Fatalf("unexpected buffered content after first write: %q", got)
	}
}

func TestWSNodeCreateNotebook(t *testing.T) {
	var wroteData []byte
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			wroteData = append([]byte(nil), data...)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeNotebook, Path: filePath}}, nil
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, _, _, errno := root.Create(context.Background(), "note.py", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}
	if string(wroteData) != "# Databricks notebook source\n" {
		t.Fatalf("unexpected notebook content: %q", string(wroteData))
	}
}

func TestWSNodeCreateAllowsPostCreateTimestampNoOp(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}

	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}

	node, ok := child.Operations().(*WSNode)
	if !ok {
		t.Fatalf("expected WSNode child, got %T", child.Operations())
	}

	setattr := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
		Valid: fuse.FATTR_ATIME | fuse.FATTR_MTIME,
		Atime: uint64(time.Now().Unix()),
		Mtime: uint64(time.Now().Unix()),
	}}
	setattrOut := &fuse.AttrOut{}
	if errno := node.Setattr(context.Background(), nil, setattr, setattrOut); errno != 0 {
		t.Fatalf("expected initial timestamp-only setattr to succeed, got %d", errno)
	}
	if setattrOut.Size != 0 {
		t.Fatalf("expected size 0, got %d", setattrOut.Size)
	}

	if errno := node.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed: %d", errno)
	}

	if errno := node.Setattr(context.Background(), nil, setattr, &fuse.AttrOut{}); errno != syscall.ENOTSUP {
		t.Fatalf("expected timestamp-only setattr after release to fail with ENOTSUP, got %d", errno)
	}
}

func TestWSNodeCreateRejectsTimestampOnlyAfterWrite(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte{}, nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}

	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}

	node, ok := child.Operations().(*WSNode)
	if !ok {
		t.Fatalf("expected WSNode child, got %T", child.Operations())
	}

	if _, errno := node.Write(context.Background(), nil, []byte("touch"), 0); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	}

	setattr := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
		Valid: fuse.FATTR_ATIME | fuse.FATTR_MTIME,
		Atime: uint64(time.Now().Unix()),
		Mtime: uint64(time.Now().Unix()),
	}}
	if errno := node.Setattr(context.Background(), nil, setattr, &fuse.AttrOut{}); errno != syscall.ENOTSUP {
		t.Fatalf("expected timestamp-only setattr after write to fail with ENOTSUP, got %d", errno)
	}
}

func TestWSNodeCreateNotebookFirstWriteReplacesScaffold(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeNotebook,
				Path:       "/note",
				Language:   workspace.LanguagePython,
			}}, nil
		},
	}

	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, _, _, errno := root.Create(context.Background(), "note.py", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}

	node, ok := child.Operations().(*WSNode)
	if !ok {
		t.Fatalf("expected WSNode child, got %T", child.Operations())
	}

	if _, errno := node.Write(context.Background(), nil, []byte("print('hello')\n"), 0); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	}
	if got := string(node.buf.Data); got != "print('hello')\n" {
		t.Fatalf("unexpected buffered content after first write: %q", got)
	}
}

func TestWSNodeCreateInvalidName(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, _, _, errno := root.Create(context.Background(), "..", 0, 0644, out)
	if errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}

func TestWSNodeMkdir(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		MkdirFunc: func(ctx context.Context, dirPath string) error { return nil },
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, true), nil
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	inode, errno := root.Mkdir(context.Background(), "newdir", 0755, out)
	if errno != 0 || inode == nil {
		t.Fatalf("Mkdir failed: errno=%d inode=%v", errno, inode)
	}
}

func TestWSNodeMkdirInvalidName(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, errno := root.Mkdir(context.Background(), "..", 0755, out)
	if errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}

func TestWSNodeMkdirUnexpectedType(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		MkdirFunc: func(ctx context.Context, dirPath string) error { return nil },
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return dummyFileInfo{}, nil
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, errno := root.Mkdir(context.Background(), "newdir", 0755, out)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
}

func TestWSNodeUnlinkRemovesCache(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := filecache.NewDiskCache(tmpDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("cache init: %v", err)
	}
	modTime := time.Now()
	remotePath := "/dir/note"
	if _, err := cache.Set(remotePath, []byte("data"), modTime); err != nil {
		t.Fatalf("cache set: %v", err)
	}

	var deletedPath string
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path:       "/dir/note",
				ObjectType: workspace.ObjectTypeNotebook,
				Language:   workspace.LanguagePython,
			}}, nil
		},
		DeleteFunc: func(ctx context.Context, filePath string, recursive bool) error {
			deletedPath = filePath
			return nil
		},
	}
	root := &WSNode{
		wfClient:  api,
		diskCache: cache,
		fileInfo:  databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeDirectory, Path: "/dir"}},
	}

	errno := root.Unlink(context.Background(), "note.py")
	if errno != 0 {
		t.Fatalf("Unlink failed: %d", errno)
	}
	if deletedPath != "/dir/note.py" {
		t.Fatalf("unexpected delete path: %s", deletedPath)
	}
	_, _, found := cache.Get(remotePath, modTime)
	if found {
		t.Fatal("expected cache entry deleted")
	}
}

func TestWSNodeRmdirNotFound(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	if errno := root.Rmdir(context.Background(), "missing"); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %d", errno)
	}
}

func TestWSNodeRmdirSuccess(t *testing.T) {
	var deletedPath string
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, true), nil
		},
		DeleteFunc: func(ctx context.Context, filePath string, recursive bool) error {
			deletedPath = filePath
			return nil
		},
	}
	root := newTestRootNode(t, api)
	if errno := root.Rmdir(context.Background(), "dir"); errno != 0 {
		t.Fatalf("Rmdir failed: %d", errno)
	}
	if deletedPath != "/dir" {
		t.Fatalf("unexpected delete path: %s", deletedPath)
	}
}

func TestWSNodeRmdirNotEmpty(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, true), nil
		},
		DeleteFunc: func(ctx context.Context, filePath string, recursive bool) error {
			return &apierr.APIError{
				StatusCode: 400,
				ErrorCode:  "DIRECTORY_NOT_EMPTY",
				Message:    "Folder (/dir) is not empty",
			}
		},
	}
	root := newTestRootNode(t, api)
	if errno := root.Rmdir(context.Background(), "dir"); errno != syscall.ENOTEMPTY {
		t.Fatalf("expected ENOTEMPTY, got %d", errno)
	}
}

type dummyParent struct{ fs.Inode }

func TestWSNodeRenameInvalidParent(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)

	errno := root.Rename(context.Background(), "file", &dummyParent{}, "new", 0)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
}

func TestWSNodeRenameRemovesCache(t *testing.T) {
	ctx := context.Background()
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			if filePath == "/file.py" {
				return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/file",
					ObjectType: workspace.ObjectTypeNotebook,
					Language:   workspace.LanguagePython,
				}}, nil
			}
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			return nil
		},
	}

	root := newTestRootNode(t, api)
	destNode := &WSNode{wfClient: api, fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeDirectory, Path: "/dest"}}}
	destInode := root.NewPersistentInode(ctx, destNode, fs.StableAttr{Mode: syscall.S_IFDIR, Ino: stableIno(destNode.fileInfo)})
	root.AddChild("dest", destInode, false)

	cacheDir := t.TempDir()
	cache, err := filecache.NewDiskCache(cacheDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("cache init: %v", err)
	}
	root.diskCache = cache
	remotePath := "/file"
	modTime := time.Now()
	if _, err := cache.Set(remotePath, []byte("data"), modTime); err != nil {
		t.Fatalf("cache set: %v", err)
	}

	errno := root.Rename(ctx, "file.py", destNode, "renamed.py", 0)
	if errno != 0 {
		t.Fatalf("Rename failed: %d", errno)
	}
	_, _, found := cache.Get(remotePath, modTime)
	if found {
		t.Fatal("expected old cache entry removed")
	}
}

func TestValidateChildPathRejectsSeparators(t *testing.T) {
	_, err := validateChildPath("/dir", "bad/name")
	if err == nil {
		t.Fatal("expected error for path separator")
	}
	_, err = validateChildPath("/dir", "bad\\name")
	if err == nil {
		t.Fatal("expected error for path separator")
	}
}

func TestValidateChildPathRootChild(t *testing.T) {
	path, err := validateChildPath("/", "child")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path != "/child" {
		t.Fatalf("unexpected path: %s", path)
	}
}

func TestWSNodeUnlinkInvalidName(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	if errno := root.Unlink(context.Background(), "bad/name"); errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}

func TestWSNodeMkdirStatError(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		MkdirFunc: func(ctx context.Context, dirPath string) error { return nil },
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, errno := root.Mkdir(context.Background(), "newdir", 0755, out)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
}

func TestWSNodeCreateMapsMissingParentToENOENT(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return &apierr.APIError{
				StatusCode: 400,
				ErrorCode:  "UNKNOWN",
				Message:    "RESOURCE_DOES_NOT_EXIST: The parent folder (/missing) does not exist.",
			}
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %d", errno)
	}
}

func TestWSNodeCreateMapsPermissionDeniedToEACCES(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return apierr.ErrPermissionDenied
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != syscall.EACCES {
		t.Fatalf("expected EACCES, got %d", errno)
	}
}

func TestWSNodeCreateStatError(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error { return nil },
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	child, fh, flags, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != 0 {
		t.Fatalf("expected create fallback success, got errno=%d", errno)
	}
	if child == nil || fh == nil {
		t.Fatalf("expected non-nil child and file handle, got child=%v fh=%v", child, fh)
	}
	if flags != fuse.FOPEN_KEEP_CACHE {
		t.Fatalf("expected KEEP_CACHE, got %d", flags)
	}
	childNode := child.Operations().(*WSNode)
	if childNode.fileInfo.Path != "/file.txt" {
		t.Fatalf("expected fallback path /file.txt, got %s", childNode.fileInfo.Path)
	}
	if childNode.fileInfo.Size() != 0 {
		t.Fatalf("expected fallback size 0, got %d", childNode.fileInfo.Size())
	}
	if childNode.fileInfo.ObjectType != workspace.ObjectTypeFile {
		t.Fatalf("expected fallback object type FILE, got %s", childNode.fileInfo.ObjectType)
	}
}

func TestWSNodeUnlinkNotFound(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	if errno := root.Unlink(context.Background(), "missing"); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %d", errno)
	}
}

func TestWSNodeRenameInvalidName(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	dest := newTestRootNode(t, api)
	if errno := root.Rename(context.Background(), "bad/name", dest, "new", 0); errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}

func TestWSNodeRenameConflictMapsToEEXIST(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
		RenameFunc: func(ctx context.Context, sourcePath string, destinationPath string) error {
			return &apierr.APIError{
				StatusCode: 409,
				ErrorCode:  "RESOURCE_ALREADY_EXISTS",
				Message:    "destination already exists",
			}
		},
	}
	root := newTestRootNode(t, api)
	dest := newTestRootNode(t, api)
	if errno := root.Rename(context.Background(), "file.txt", dest, "new.txt", 0); errno != syscall.EEXIST {
		t.Fatalf("expected EEXIST, got %d", errno)
	}
}

func TestWSNodeUnlinkDirectoryNameReject(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	if errno := root.Unlink(context.Background(), string(filepath.Separator)); errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}
