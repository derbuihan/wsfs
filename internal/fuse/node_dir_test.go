package fuse

import (
	"context"
	iofs "io/fs"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
)

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
	if child.ownerUid != root.ownerUid || child.restrictAccess != root.restrictAccess {
		t.Fatal("child did not inherit access config")
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
	if fh != nil {
		t.Fatalf("expected nil file handle, got %v", fh)
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
	child, _, _, errno := root.Create(context.Background(), "note.ipynb", 0, 0644, out)
	if errno != 0 || child == nil {
		t.Fatalf("Create failed: errno=%d child=%v", errno, child)
	}
	if string(wroteData) != `{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":4}` {
		t.Fatalf("unexpected notebook content: %q", string(wroteData))
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
			return databricks.NewTestFileInfo(filePath, 0, false), nil
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

	errno := root.Unlink(context.Background(), "note.ipynb")
	if errno != 0 {
		t.Fatalf("Unlink failed: %d", errno)
	}
	if deletedPath != "/dir/note.ipynb" {
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

	errno := root.Rename(ctx, "file.ipynb", destNode, "renamed.ipynb", 0)
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

func TestWSNodeCreateStatError(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error { return nil },
		StatFunc: func(ctx context.Context, filePath string) (iofs.FileInfo, error) {
			return nil, iofs.ErrNotExist
		},
	}
	root := newTestRootNode(t, api)
	out := &fuse.EntryOut{}
	_, _, _, errno := root.Create(context.Background(), "file.txt", 0, 0644, out)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
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

func TestWSNodeUnlinkDirectoryNameReject(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{}
	root := newTestRootNode(t, api)
	if errno := root.Unlink(context.Background(), string(filepath.Separator)); errno != syscall.EINVAL {
		t.Fatalf("expected EINVAL, got %d", errno)
	}
}
