package fuse

import (
	"context"
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
)

func TestWSNodeTruncateLockedShrinks(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Size:       10,
		}},
		buf: fileBuffer{Data: []byte("0123456789")},
	}

	n.truncateLocked(5)

	if got := string(n.buf.Data); got != "01234" {
		t.Fatalf("unexpected data after truncate: %q", got)
	}
	if got := n.fileInfo.Size(); got != 5 {
		t.Fatalf("unexpected size after truncate: %d", got)
	}
	if !n.buf.Dirty {
		t.Fatal("expected buffer to be dirty after truncate")
	}
}

func TestWSNodeWriteExtendsBuffer(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte("hi"), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test",
			Size:       2,
		}},
	}

	_, errno := n.Write(context.Background(), nil, []byte("!"), 5)
	if errno != 0 {
		t.Fatalf("unexpected errno: %d", errno)
	}
	if got := len(n.buf.Data); got != 6 {
		t.Fatalf("unexpected buffer length: %d", got)
	}
	if got := n.buf.Data[5]; got != '!' {
		t.Fatalf("unexpected last byte: %q", got)
	}
	if got := n.fileInfo.Size(); got != 6 {
		t.Fatalf("unexpected size after write: %d", got)
	}
	if !n.buf.Dirty {
		t.Fatal("expected buffer to be dirty after write")
	}
}

// TestWSNodeRead tests basic read operations
func TestWSNodeRead(t *testing.T) {
	testData := []byte("Hello, World!")
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return testData, nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(testData)),
		}},
	}

	// Read full content
	dest := make([]byte, len(testData))
	result, errno := n.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed with errno: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != string(testData) {
		t.Errorf("Expected %q, got %q", string(testData), string(data))
	}

	// Read with offset
	dest = make([]byte, 5)
	result, errno = n.Read(context.Background(), nil, dest, 7)
	if errno != 0 {
		t.Fatalf("Read with offset failed with errno: %d", errno)
	}
	data, _ = result.Bytes(nil)
	if string(data) != "World" {
		t.Errorf("Expected 'World', got %q", string(data))
	}

	// Read beyond end
	dest = make([]byte, 100)
	result, errno = n.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read beyond end failed with errno: %d", errno)
	}
	data, _ = result.Bytes(nil)
	if len(data) != len(testData) {
		t.Errorf("Expected length %d, got %d", len(testData), len(data))
	}

	// Read starting beyond end
	dest = make([]byte, 10)
	result, errno = n.Read(context.Background(), nil, dest, 100)
	if errno != 0 {
		t.Fatalf("Read starting beyond end failed with errno: %d", errno)
	}
	data, _ = result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(data))
	}
}

// TestWSNodeWriteAtOffset tests writing at specific offsets
func TestWSNodeWriteAtOffset(t *testing.T) {
	initialData := []byte("Hello, World!")
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return initialData, nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(initialData)),
		}},
		// Pre-populate buffer to avoid ensureDataLocked reading fresh data
		buf: fileBuffer{Data: []byte("Hello, World!"), Dirty: false},
	}

	// Write at beginning
	written, errno := n.Write(context.Background(), nil, []byte("Hi"), 0)
	if errno != 0 {
		t.Fatalf("Write failed with errno: %d", errno)
	}
	if written != 2 {
		t.Errorf("Expected 2 bytes written, got %d", written)
	}
	if string(n.buf.Data[:5]) != "Hillo" {
		t.Errorf("Expected 'Hillo', got %q", string(n.buf.Data[:5]))
	}

	// Write in middle
	written, errno = n.Write(context.Background(), nil, []byte("Go"), 7)
	if errno != 0 {
		t.Fatalf("Write failed with errno: %d", errno)
	}
	if written != 2 {
		t.Errorf("Expected 2 bytes written, got %d", written)
	}
	if string(n.buf.Data) != "Hillo, Gorld!" {
		t.Errorf("Expected 'Hillo, Gorld!', got %q", string(n.buf.Data))
	}

	if !n.buf.Dirty {
		t.Error("Expected buffer to be dirty after write")
	}
}

// TestWSNodeWriteNegativeOffset tests that negative offset returns error
func TestWSNodeWriteNegativeOffset(t *testing.T) {
	n := &WSNode{
		wfClient: &databricks.FakeWorkspaceAPI{},
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       0,
		}},
		buf: fileBuffer{Data: []byte{}},
	}

	_, errno := n.Write(context.Background(), nil, []byte("test"), -1)
	if errno == 0 {
		t.Error("Expected error for negative offset")
	}
}

// TestWSNodeReadEmptyFile tests reading an empty file
func TestWSNodeReadEmptyFile(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte{}, nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/empty.txt",
			Size:       0,
		}},
	}

	dest := make([]byte, 100)
	result, errno := n.Read(context.Background(), nil, dest, 0)
	if errno != 0 {
		t.Fatalf("Read failed with errno: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if len(data) != 0 {
		t.Errorf("Expected empty result, got %d bytes", len(data))
	}
}

// TestWSNodeFlushCleanBuffer tests that flushing clean buffer does nothing
func TestWSNodeFlushCleanBuffer(t *testing.T) {
	writeCalled := false
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalled = true
			return nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("test"), Dirty: false},
	}

	errno := n.Flush(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Flush failed with errno: %d", errno)
	}
	if writeCalled {
		t.Error("Expected Write not to be called for clean buffer")
	}
}

// TestWSNodeFlushDirtyBuffer tests that flushing dirty buffer writes data
func TestWSNodeFlushDirtyBuffer(t *testing.T) {
	var writtenData []byte
	var writtenPath string
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writtenPath = filepath
			writtenData = make([]byte, len(data))
			copy(writtenData, data)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, int64(len(writtenData)), false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("new content"), Dirty: true},
	}

	errno := n.Flush(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Flush failed with errno: %d", errno)
	}
	if writtenPath != "/test.txt" {
		t.Errorf("Expected path '/test.txt', got %q", writtenPath)
	}
	if string(writtenData) != "new content" {
		t.Errorf("Expected 'new content', got %q", string(writtenData))
	}
	if n.buf.Dirty {
		t.Error("Expected buffer to be clean after flush")
	}
}

// TestWSNodeRelease tests that Release flushes and clears buffer
func TestWSNodeRelease(t *testing.T) {
	var writtenData []byte
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writtenData = make([]byte, len(data))
			copy(writtenData, data)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, int64(len(writtenData)), false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("content"), Dirty: true},
	}

	errno := n.Release(context.Background(), nil)
	if errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if string(writtenData) != "content" {
		t.Errorf("Expected 'content' to be written, got %q", string(writtenData))
	}
	if n.buf.Data != nil {
		t.Error("Expected buffer to be cleared after release")
	}
	if n.buf.Dirty {
		t.Error("Expected dirty flag to be cleared after release")
	}
}

func TestReleaseUsesLocalMetadataForImmediateLookupAndRead(t *testing.T) {
	cache, err := filecache.NewDiskCache(t.TempDir(), 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("cache init: %v", err)
	}

	freshSize := int64(0)
	freshModTime := time.Now()
	writtenData := []byte("shared content")
	statCalls := 0
	statFreshCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte{}, nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			freshSize = int64(len(data))
			freshModTime = time.Now()
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statCalls++
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path:       filePath,
				ObjectType: workspace.ObjectTypeFile,
				Size:       0,
				ModifiedAt: freshModTime.Add(-time.Second).UnixMilli(),
			}}, nil
		},
		StatFreshFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statFreshCalls++
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path:       filePath,
				ObjectType: workspace.ObjectTypeFile,
				Size:       freshSize,
				ModifiedAt: freshModTime.UnixMilli(),
			}}, nil
		},
	}

	root := newTestRootNode(t, api)
	root.diskCache = cache

	createOut := &fuse.EntryOut{}
	inode, _, _, errno := root.Create(context.Background(), "shared.txt", 0, 0644, createOut)
	if errno != 0 {
		t.Fatalf("Create failed with errno: %d", errno)
	}
	root.AddChild("shared.txt", inode, false)
	child := inode.Operations().(*WSNode)

	if written, errno := child.Write(context.Background(), nil, writtenData, 0); errno != 0 {
		t.Fatalf("Write failed with errno: %d", errno)
	} else if written != uint32(len(writtenData)) {
		t.Fatalf("expected %d bytes written, got %d", len(writtenData), written)
	}

	if errno := child.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if statCalls != 0 {
		t.Fatalf("expected stale Stat path to be unused, got %d calls", statCalls)
	}
	if statFreshCalls != 1 {
		t.Fatalf("expected StatFresh to be used only for create, got %d calls", statFreshCalls)
	}
	if child.fileInfo.Size() != int64(len(writtenData)) {
		t.Fatalf("expected child size %d after flush, got %d", len(writtenData), child.fileInfo.Size())
	}

	lookupOut := &fuse.EntryOut{}
	lookedUp, errno := root.Lookup(context.Background(), "shared.txt", lookupOut)
	if errno != 0 {
		t.Fatalf("Lookup failed with errno: %d", errno)
	}
	if lookedUp != inode {
		t.Fatal("expected lookup to reuse existing child inode")
	}
	if lookupOut.Attr.Size != uint64(len(writtenData)) {
		t.Fatalf("expected lookup size %d, got %d", len(writtenData), lookupOut.Attr.Size)
	}

	if _, _, errno := child.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}
	result, errno := child.Read(context.Background(), nil, make([]byte, len(writtenData)), 0)
	if errno != 0 {
		t.Fatalf("Read failed with errno: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != string(writtenData) {
		t.Fatalf("expected read data %q, got %q", string(writtenData), string(data))
	}
}

func TestReleaseRegularFileUsesLocalMetadataWithoutStatFresh(t *testing.T) {
	writtenData := []byte("content")
	oldModifiedAt := time.Now().Add(-time.Hour).UnixMilli()
	writeCalls := 0
	statFreshCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalls++
			return nil
		},
		StatFreshFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statFreshCalls++
			return nil, fs.ErrPermission
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       0,
			ModifiedAt: oldModifiedAt,
		}},
		buf:       fileBuffer{Data: append([]byte(nil), writtenData...), Dirty: true},
		openCount: 1,
	}

	before := time.Now()
	if errno := n.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if writeCalls != 1 {
		t.Fatalf("expected 1 write call, got %d", writeCalls)
	}
	if statFreshCalls != 0 {
		t.Fatalf("expected StatFresh to be unused for regular files, got %d calls", statFreshCalls)
	}
	if n.fileInfo.Size() != int64(len(writtenData)) {
		t.Fatalf("expected size %d after fallback, got %d", len(writtenData), n.fileInfo.Size())
	}
	if n.fileInfo.ModifiedAt <= oldModifiedAt {
		t.Fatalf("expected modified time to advance, got %d <= %d", n.fileInfo.ModifiedAt, oldModifiedAt)
	}
	if n.metadataCheckedAt.Before(before) {
		t.Fatalf("expected metadataCheckedAt to update, got %v before %v", n.metadataCheckedAt, before)
	}
	if n.buf.Data != nil {
		t.Fatal("expected buffer to be cleared after release")
	}
	if n.buf.Dirty {
		t.Fatal("expected dirty flag cleared after release")
	}
}

func TestRefreshMetadataLockedPreservesNotebookExactSize(t *testing.T) {
	modifiedAt := time.Now().UnixMilli()
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path:       "/test/notebook",
				ObjectType: workspace.ObjectTypeNotebook,
				Language:   workspace.LanguagePython,
				Size:       1,
				ModifiedAt: modifiedAt,
			}}, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{
			ObjectInfo: workspace.ObjectInfo{
				Path:       "/test/notebook",
				ObjectType: workspace.ObjectTypeNotebook,
				Language:   workspace.LanguagePython,
				Size:       44,
				ModifiedAt: modifiedAt,
			},
			NotebookSizeComputed: true,
		},
	}

	changed, errno := n.refreshMetadataLocked(context.Background(), true)
	if errno != 0 {
		t.Fatalf("refreshMetadataLocked failed: %d", errno)
	}
	if changed {
		t.Fatal("did not expect metadata refresh to treat preserved exact size as a change")
	}
	if n.fileInfo.Size() != 44 {
		t.Fatalf("expected preserved exact size 44, got %d", n.fileInfo.Size())
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected notebook exact size to stay computed")
	}
}

// TestOpenReleaseFlushesWhenLastHandleClosed verifies flush happens only on last close.
func TestOpenReleaseFlushesWhenLastHandleClosed(t *testing.T) {
	var writeCalls int
	var lastWrittenSize int64
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalls++
			lastWrittenSize = int64(len(data))
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, lastWrittenSize, false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty")},
	}

	n.mu.Lock()
	n.markDirtyLocked(dirtyData)
	n.mu.Unlock()

	if _, _, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}
	if _, _, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	if writeCalls != 0 {
		t.Fatalf("Expected no writes before release, got %d", writeCalls)
	}

	if errno := n.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if writeCalls != 0 {
		t.Fatalf("Expected no writes after first release, got %d", writeCalls)
	}

	if errno := n.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if writeCalls != 1 {
		t.Fatalf("Expected 1 write after last release, got %d", writeCalls)
	}
}

func TestFlushSkipsWhenOpenCountPositive(t *testing.T) {
	var writeCalls int
	var lastWrittenSize int64
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalls++
			lastWrittenSize = int64(len(data))
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, lastWrittenSize, false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty")},
	}

	n.mu.Lock()
	n.markDirtyLocked(dirtyData)
	n.mu.Unlock()

	if _, _, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	if errno := n.Flush(context.Background(), nil); errno != 0 {
		t.Fatalf("Flush failed with errno: %d", errno)
	}
	if writeCalls != 0 {
		t.Fatalf("Expected no writes while openCount > 0, got %d", writeCalls)
	}

	if errno := n.Release(context.Background(), nil); errno != 0 {
		t.Fatalf("Release failed with errno: %d", errno)
	}
	if writeCalls != 1 {
		t.Fatalf("Expected 1 write after release, got %d", writeCalls)
	}
}

func TestFsyncFlushesEvenWithOpenCount(t *testing.T) {
	var writeCalls int
	var lastWrittenSize int64
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalls++
			lastWrittenSize = int64(len(data))
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, lastWrittenSize, false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty")},
	}

	n.mu.Lock()
	n.markDirtyLocked(dirtyData)
	n.mu.Unlock()

	if _, _, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	if errno := n.Fsync(context.Background(), nil, 0); errno != 0 {
		t.Fatalf("Fsync failed with errno: %d", errno)
	}
	if writeCalls != 1 {
		t.Fatalf("Expected 1 write after fsync, got %d", writeCalls)
	}
	if n.buf.Dirty {
		t.Fatal("Expected dirty flag to be cleared after fsync")
	}
}

// TestWSNodeFsync tests that Fsync works like Flush
func TestWSNodeFsync(t *testing.T) {
	var writtenData []byte
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writtenData = make([]byte, len(data))
			copy(writtenData, data)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, int64(len(writtenData)), false), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("synced content"), Dirty: true},
	}

	errno := n.Fsync(context.Background(), nil, 0)
	if errno != 0 {
		t.Fatalf("Fsync failed with errno: %d", errno)
	}
	if string(writtenData) != "synced content" {
		t.Errorf("Expected 'synced content', got %q", string(writtenData))
	}
	if n.buf.Dirty {
		t.Error("Expected buffer to be clean after fsync")
	}
}

// TestWSNodeSetattr tests attribute modification
func TestWSNodeSetattr(t *testing.T) {
	var lastWrittenSize int64 = 12
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte("test content"), nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			lastWrittenSize = int64(len(data))
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, lastWrittenSize, false), nil
		},
		CacheSetFunc: func(path string, info fs.FileInfo) {},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       12,
		}},
	}

	// Test size change (truncate)
	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE | fuse.FATTR_MODE
	in.Size = 5
	in.Mode = 0600
	out := &fuse.AttrOut{}

	errno := n.Setattr(context.Background(), nil, in, out)
	if errno != 0 {
		t.Fatalf("Setattr failed with errno: %d", errno)
	}
	if out.Size != 5 {
		t.Errorf("Expected size 5, got %d", out.Size)
	}
	if got := out.Mode & 0777; got != fileMode {
		t.Errorf("expected synthetic mode %o, got %o", fileMode, got)
	}
}

func TestWSNodeSetattrAcceptsModeOnlyAsNoOp(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       12,
		}},
	}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_MODE
	in.Mode = 0600
	out := &fuse.AttrOut{}

	if errno := n.Setattr(context.Background(), nil, in, out); errno != 0 {
		t.Fatalf("expected success, got errno %d", errno)
	}
	if out.Size != 12 {
		t.Fatalf("expected size 12, got %d", out.Size)
	}
	if got := out.Mode & 0777; got != fileMode {
		t.Fatalf("expected synthetic mode %o, got %o", fileMode, got)
	}
}

func TestWSNodeSetattrRejectsTimestampOnly(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       12,
		}},
	}

	testCases := []struct {
		name string
		in   *fuse.SetAttrIn
	}{
		{
			name: "mtime only",
			in: &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
				Valid: fuse.FATTR_MTIME,
				Mtime: uint64(time.Now().Unix()),
			}},
		},
		{
			name: "atime only",
			in: &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
				Valid: fuse.FATTR_ATIME,
				Atime: uint64(time.Now().Unix()),
			}},
		},
		{
			name: "atime and mtime",
			in: &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
				Valid: fuse.FATTR_ATIME | fuse.FATTR_MTIME,
				Atime: uint64(time.Now().Unix()),
				Mtime: uint64(time.Now().Unix()),
			}},
		},
		{
			name: "mode and mtime",
			in: func() *fuse.SetAttrIn {
				in := &fuse.SetAttrIn{}
				in.Valid = fuse.FATTR_MODE | fuse.FATTR_MTIME
				in.Mode = 0600
				in.Mtime = uint64(time.Now().Unix())
				return in
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := &fuse.AttrOut{}
			if errno := n.Setattr(context.Background(), nil, tc.in, out); errno != syscall.ENOTSUP {
				t.Fatalf("expected ENOTSUP, got %d", errno)
			}
		})
	}
}

func TestWSNodeSetattrRejectsUIDAndGID(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       12,
		}},
	}

	testCases := []struct {
		name string
		in   *fuse.SetAttrIn
	}{
		{
			name: "uid only",
			in: &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
				Valid: fuse.FATTR_UID,
				Owner: fuse.Owner{Uid: 1234},
			}},
		},
		{
			name: "gid only",
			in: &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
				Valid: fuse.FATTR_GID,
				Owner: fuse.Owner{Gid: 5678},
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out := &fuse.AttrOut{}
			if errno := n.Setattr(context.Background(), nil, tc.in, out); errno != syscall.ENOTSUP {
				t.Fatalf("expected ENOTSUP, got %d", errno)
			}
		})
	}
}

// TestSetattrTruncateWithoutOpenFlushes ensures truncate without open handle flushes immediately.
func TestSetattrTruncateWithoutOpenFlushes(t *testing.T) {
	var writeCalls int
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCalls++
			if len(data) != 0 {
				t.Fatalf("expected empty write, got %d bytes", len(data))
			}
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
		CacheInvalidateFunc: func(filePath string) {},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       5,
		}},
	}

	in := &fuse.SetAttrIn{}
	in.Valid = fuse.FATTR_SIZE
	in.Size = 0
	out := &fuse.AttrOut{}

	errno := n.Setattr(context.Background(), nil, in, out)
	if errno != 0 {
		t.Fatalf("Setattr failed with errno: %d", errno)
	}
	if out.Size != 0 {
		t.Errorf("Expected size 0, got %d", out.Size)
	}
	if writeCalls != 1 {
		t.Fatalf("Expected 1 write call, got %d", writeCalls)
	}
}

// TestWSNodeGetattrFile tests getting attributes of a file
func TestWSNodeGetattrFile(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       100,
			ModifiedAt: time.Now().UnixMilli(),
		}},
	}

	out := &fuse.AttrOut{}
	errno := n.Getattr(context.Background(), nil, out)
	if errno != 0 {
		t.Fatalf("Getattr failed with errno: %d", errno)
	}
	if out.Size != 100 {
		t.Errorf("Expected size 100, got %d", out.Size)
	}
	if out.Mode&syscall.S_IFDIR != 0 {
		t.Error("File should not be a directory")
	}
}

// TestWSNodeGetattrDirectory tests getting attributes of a directory
func TestWSNodeGetattrDirectory(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/mydir",
			ModifiedAt: time.Now().UnixMilli(),
		}},
	}

	out := &fuse.AttrOut{}
	errno := n.Getattr(context.Background(), nil, out)
	if errno != 0 {
		t.Fatalf("Getattr failed with errno: %d", errno)
	}
	if out.Mode&syscall.S_IFDIR == 0 {
		t.Error("Directory should have directory flag set")
	}
}

func TestWSNodeGetattrUsesMountOwnerIDs(t *testing.T) {
	n := &WSNode{
		ownerUid: 123,
		ownerGid: 456,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/owned.txt",
			Size:       7,
			ModifiedAt: time.Now().UnixMilli(),
		}},
	}

	out := &fuse.AttrOut{}
	if errno := n.Getattr(context.Background(), nil, out); errno != 0 {
		t.Fatalf("Getattr failed with errno: %d", errno)
	}
	if out.Uid != 123 || out.Gid != 456 {
		t.Fatalf("expected uid/gid 123/456, got %d/%d", out.Uid, out.Gid)
	}
}

func TestWSNodeGetattrNotebookLearnsExactSize(t *testing.T) {
	notebookContent := []byte("# Databricks notebook source\nprint('hello')\n")
	readAllCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return notebookContent, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeNotebook,
			Path:       "/test/notebook",
			Language:   workspace.LanguagePython,
			Size:       1,
			ModifiedAt: time.Now().UnixMilli(),
		}},
		metadataCheckedAt: time.Now(),
	}

	out := &fuse.AttrOut{}
	if errno := n.Getattr(context.Background(), nil, out); errno != 0 {
		t.Fatalf("Getattr failed with errno: %d", errno)
	}
	if out.Size != uint64(len(notebookContent)) {
		t.Fatalf("expected exact notebook size %d, got %d", len(notebookContent), out.Size)
	}
	if readAllCalls != 1 {
		t.Fatalf("expected one notebook read during Getattr, got %d", readAllCalls)
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected exact notebook size to be learned during Getattr")
	}
}

// TestWSNodeAccess tests Access without restriction (allow all)
func TestWSNodeAccess(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		restrictAccess: false, // No access control
	}

	// Test various access masks - all should succeed
	masks := []uint32{0, 1, 2, 4, 7}
	for _, mask := range masks {
		errno := n.Access(context.Background(), mask)
		if errno != 0 {
			t.Errorf("Access(mask=%d) returned errno %d, expected 0", mask, errno)
		}
	}
}

// TestWSNodeAccessRestricted tests Access with UID-based restriction
func TestWSNodeAccessRestricted(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		ownerUid:       1000,
		ownerGid:       1001,
		restrictAccess: true, // Access control enabled
	}

	// Without FUSE context, access should be denied
	// (context.Background() doesn't have fuse.Caller)
	errno := n.Access(context.Background(), 0)
	if errno != syscall.EACCES {
		t.Errorf("Access with restricted mode but no FUSE context should return EACCES, got %d", errno)
	}
}

// TestWSNodeAccessRestrictedInheritance tests that child nodes inherit access settings
func TestWSNodeAccessRestrictedInheritance(t *testing.T) {
	parent := &WSNode{
		ownerUid:       1000,
		ownerGid:       1001,
		restrictAccess: true,
	}

	// Simulate child node creation pattern
	child := &WSNode{
		ownerUid:       parent.ownerUid,
		ownerGid:       parent.ownerGid,
		restrictAccess: parent.restrictAccess,
	}

	if child.ownerUid != parent.ownerUid {
		t.Errorf("Child ownerUid %d != parent ownerUid %d", child.ownerUid, parent.ownerUid)
	}
	if child.ownerGid != parent.ownerGid {
		t.Errorf("Child ownerGid %d != parent ownerGid %d", child.ownerGid, parent.ownerGid)
	}
	if child.restrictAccess != parent.restrictAccess {
		t.Errorf("Child restrictAccess %v != parent restrictAccess %v", child.restrictAccess, parent.restrictAccess)
	}
}

// TestWSNodeStatfs tests that Statfs returns expected values
func TestWSNodeStatfs(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/",
		}},
	}

	out := &fuse.StatfsOut{}
	errno := n.Statfs(context.Background(), out)
	if errno != 0 {
		t.Fatalf("Statfs returned errno: %d", errno)
	}

	if out.Bsize != 4096 {
		t.Errorf("Expected Bsize 4096, got %d", out.Bsize)
	}
	if out.NameLen != 255 {
		t.Errorf("Expected NameLen 255, got %d", out.NameLen)
	}
	if out.Blocks == 0 {
		t.Error("Expected non-zero Blocks")
	}
}

// TestWSNodeOpenTrunc tests Open with O_TRUNC flag
func TestWSNodeOpenTrunc(t *testing.T) {
	n := &WSNode{
		wfClient: &databricks.FakeWorkspaceAPI{},
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       100,
		}},
		buf: fileBuffer{Data: []byte("existing content")},
	}

	_, _, errno := n.Open(context.Background(), syscall.O_TRUNC|syscall.O_WRONLY)
	if errno != 0 {
		t.Fatalf("Open with O_TRUNC failed with errno: %d", errno)
	}

	// Buffer should be empty after O_TRUNC
	if len(n.buf.Data) != 0 {
		t.Errorf("Expected empty buffer after O_TRUNC, got %d bytes", len(n.buf.Data))
	}
	if n.fileInfo.Size() != 0 {
		t.Errorf("Expected size 0 after O_TRUNC, got %d", n.fileInfo.Size())
	}
	if !n.buf.Dirty {
		t.Error("Expected buffer to be dirty after O_TRUNC")
	}
}

// TestWSNodeOpenDirectory tests that Open on directory returns EISDIR
func TestWSNodeOpenDirectory(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/mydir",
		}},
	}

	_, _, errno := n.Open(context.Background(), 0)
	if errno != syscall.EISDIR {
		t.Errorf("Expected EISDIR, got errno: %d", errno)
	}
}

// TestWSNodeReaddir tests directory listing
func TestWSNodeReaddir(t *testing.T) {
	entries := []fs.DirEntry{
		databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/file1.txt",
			ObjectType: workspace.ObjectTypeFile,
		}}},
		databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/subdir",
			ObjectType: workspace.ObjectTypeDirectory,
		}}},
	}

	api := &databricks.FakeWorkspaceAPI{
		ReadDirFunc: func(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
			return entries, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/test",
		}},
	}

	stream, errno := n.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir failed with errno: %d", errno)
	}
	if stream == nil {
		t.Fatal("Expected non-nil stream")
	}
}

// TestWSNodeReaddirNotDir tests that Readdir on file returns ENOTDIR
func TestWSNodeReaddirNotDir(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
	}

	_, errno := n.Readdir(context.Background())
	if errno != syscall.ENOTDIR {
		t.Errorf("Expected ENOTDIR, got errno: %d", errno)
	}
}

// TestWSNodeUnlinkDirectory tests that Unlink on directory returns EISDIR
func TestWSNodeUnlinkDirectory(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo("/dir/subdir", 0, true), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/dir",
		}},
	}

	errno := n.Unlink(context.Background(), "subdir")
	if errno != syscall.EISDIR {
		t.Errorf("Expected EISDIR when unlinking directory, got errno: %d", errno)
	}
}

// TestWSNodeRmdirFile tests that Rmdir on file returns ENOTDIR
func TestWSNodeRmdirFile(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo("/dir/file.txt", 100, false), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/dir",
		}},
	}

	errno := n.Rmdir(context.Background(), "file.txt")
	if errno != syscall.ENOTDIR {
		t.Errorf("Expected ENOTDIR when rmdir on file, got errno: %d", errno)
	}
}

// TestWSNodeOnForgetClean tests that OnForget clears clean buffer
func TestWSNodeOnForgetClean(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("test data"), Dirty: false},
	}

	n.OnForget()

	if n.buf.Data != nil {
		t.Error("Expected buffer to be cleared on forget for clean buffer")
	}
}

// TestWSNodeOnForgetDirty tests that OnForget preserves dirty buffer
func TestWSNodeOnForgetDirty(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty data"), Dirty: true},
	}

	n.OnForget()

	if n.buf.Data == nil {
		t.Error("Expected dirty buffer to be preserved on forget")
	}
}

// ============================================================================
// Remote Modification Detection Tests
// ============================================================================

// TestOpenDetectsRemoteModification verifies that Open() drops clean cached data when remote metadata changes.
func TestOpenDetectsRemoteModification(t *testing.T) {
	originalTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()
	originalData := []byte("original content")
	newData := []byte("modified content")
	readAllCallCount := 0

	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			// Return newer modification time
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       "/test.txt",
				Size:       int64(len(newData)),
				ModifiedAt: newTime.UnixMilli(),
			}}, nil
		},
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCallCount++
			return newData, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(originalData)),
			ModifiedAt: originalTime.UnixMilli(),
		}},
		buf:               fileBuffer{Data: originalData, Dirty: false},
		metadataCheckedAt: time.Now().Add(-2 * time.Second),
	}

	_, openFlags, errno := n.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}
	if openFlags&fuse.FOPEN_DIRECT_IO == 0 {
		t.Fatalf("expected DIRECT_IO after remote metadata change, got flags=%d", openFlags)
	}

	if n.buf.Data != nil {
		t.Fatal("expected clean in-memory buffer to be dropped on metadata refresh")
	}
	if n.fileInfo.ModTime().UnixMilli() != newTime.UnixMilli() {
		t.Fatalf("expected modtime to be refreshed")
	}
	if readAllCallCount != 0 {
		t.Fatalf("expected Open to defer content fetch, got %d ReadAll calls", readAllCallCount)
	}

	result, errno := n.Read(context.Background(), nil, make([]byte, len(newData)), 0)
	if errno != 0 {
		t.Fatalf("Read failed: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != string(newData) {
		t.Fatalf("expected refreshed data %q, got %q", string(newData), string(data))
	}
	if readAllCallCount != 1 {
		t.Fatalf("expected one deferred ReadAll call, got %d", readAllCallCount)
	}
}

func TestOpenReadOnlyWithinTTLUsesCachedMetadata(t *testing.T) {
	modTime := time.Now()
	content := []byte("cached content")
	statCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statCalls++
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       "/test.txt",
				Size:       int64(len(content)),
				ModifiedAt: modTime.UnixMilli(),
			}}, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(content)),
			ModifiedAt: modTime.UnixMilli(),
		}},
		buf:               fileBuffer{Data: content, Dirty: false},
		metadataCheckedAt: time.Now(),
	}

	for attempt := 0; attempt < 2; attempt++ {
		_, openFlags, errno := n.Open(context.Background(), 0)
		if errno != 0 {
			t.Fatalf("Open failed with errno: %d", errno)
		}
		if openFlags&fuse.FOPEN_KEEP_CACHE == 0 {
			t.Fatalf("expected KEEP_CACHE for warm read-only open, got flags=%d", openFlags)
		}
	}

	if statCalls != 0 {
		t.Fatalf("expected no Stat calls within metadata TTL, got %d", statCalls)
	}
	if string(n.buf.Data) != string(content) {
		t.Fatalf("expected clean buffer to be preserved")
	}
}

// TestOpenPreservesDirtyBuffer verifies that Open() does not invalidate dirty buffer
func TestOpenPreservesDirtyBuffer(t *testing.T) {
	originalTime := time.Now().Add(-1 * time.Hour)
	newTime := time.Now()
	localData := []byte("local modifications")

	statCalled := false
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statCalled = true
			// Return newer modification time
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       "/test.txt",
				Size:       100,
				ModifiedAt: newTime.UnixMilli(),
			}}, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(localData)),
			ModifiedAt: originalTime.UnixMilli(),
		}},
		buf:               fileBuffer{Data: localData, Dirty: true}, // Buffer is dirty
		metadataCheckedAt: time.Now().Add(-2 * time.Second),
	}

	_, _, errno := n.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	// Dirty buffer should be preserved
	if string(n.buf.Data) != string(localData) {
		t.Error("Expected dirty buffer to be preserved")
	}

	// Stat should not be called for dirty buffer
	if statCalled {
		t.Error("Expected Stat not to be called for dirty buffer")
	}
}

// TestOpenNoChangeWhenRemoteNotModified verifies Open() keeps cache when remote is unchanged
func TestOpenNoChangeWhenRemoteNotModified(t *testing.T) {
	sameTime := time.Now()
	originalData := []byte("original content")
	statCalls := 0
	readAllCalled := false

	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statCalls++
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       "/test.txt",
				Size:       int64(len(originalData)),
				ModifiedAt: sameTime.UnixMilli(),
			}}, nil
		},
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalled = true
			return []byte("should not be called"), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       int64(len(originalData)),
			ModifiedAt: sameTime.UnixMilli(),
		}},
		buf:               fileBuffer{Data: originalData, Dirty: false},
		metadataCheckedAt: time.Now().Add(-2 * time.Second),
	}

	_, openFlags, errno := n.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}
	if openFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Fatalf("expected KEEP_CACHE when metadata is unchanged, got flags=%d", openFlags)
	}

	// Buffer should still have original data
	if string(n.buf.Data) != string(originalData) {
		t.Error("Expected buffer to keep original data")
	}

	// ReadAll should not be called since remote is unchanged
	if readAllCalled {
		t.Error("Expected ReadAll not to be called when remote is unchanged")
	}
	if statCalls != 1 {
		t.Errorf("expected one Stat call after metadata TTL expiry, got %d", statCalls)
	}
}

func TestOpenReadOnlyDefersRemoteRead(t *testing.T) {
	readAllCalls := 0
	modTime := time.Now()
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       "/test.txt",
				Size:       5,
				ModifiedAt: modTime.UnixMilli(),
			}}, nil
		},
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return []byte("hello"), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       5,
			ModifiedAt: modTime.UnixMilli(),
		}},
		metadataCheckedAt: time.Now(),
	}

	if _, openFlags, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed: %d", errno)
	} else if openFlags&fuse.FOPEN_KEEP_CACHE == 0 {
		t.Fatalf("expected KEEP_CACHE for unchanged read-only open, got flags=%d", openFlags)
	}
	if readAllCalls != 0 {
		t.Fatalf("expected no eager reads on Open, got %d", readAllCalls)
	}

	result, errno := n.Read(context.Background(), nil, make([]byte, 5), 0)
	if errno != 0 {
		t.Fatalf("Read failed: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != "hello" {
		t.Fatalf("unexpected data: %q", string(data))
	}
	if readAllCalls != 1 {
		t.Fatalf("expected one deferred read, got %d", readAllCalls)
	}
}

func TestOpenReadOnlyNotebookWithoutExactSizeLearnsSizeAndKeepsCache(t *testing.T) {
	notebookContent := []byte("# Databricks notebook source\nprint('hello')\n")
	modTime := time.Now()
	readAllCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return notebookContent, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeNotebook,
			Path:       "/test/notebook",
			Language:   workspace.LanguagePython,
			Size:       1,
			ModifiedAt: modTime.UnixMilli(),
		}},
		metadataCheckedAt: time.Now(),
	}

	if _, openFlags, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed: %d", errno)
	} else {
		if openFlags&fuse.FOPEN_DIRECT_IO != 0 {
			t.Fatalf("did not expect DIRECT_IO for unchanged notebook open, got flags=%d", openFlags)
		}
		if openFlags&fuse.FOPEN_KEEP_CACHE == 0 {
			t.Fatalf("expected KEEP_CACHE for notebook after exact size learning, got flags=%d", openFlags)
		}
	}
	if readAllCalls != 1 {
		t.Fatalf("expected one notebook read during Open, got %d", readAllCalls)
	}
	if n.fileInfo.Size() != int64(len(notebookContent)) {
		t.Fatalf("expected learned notebook size %d after Open, got %d", len(notebookContent), n.fileInfo.Size())
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected notebook exact size to be learned during Open")
	}

	result, errno := n.Read(context.Background(), nil, make([]byte, len(notebookContent)+8), 0)
	if errno != 0 {
		t.Fatalf("Read failed: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != string(notebookContent) {
		t.Fatalf("unexpected notebook data: %q", string(data))
	}
	if readAllCalls != 1 {
		t.Fatalf("expected no extra notebook read after Open, got %d", readAllCalls)
	}
}

func TestEnsureDataLockedUsesCachedNotebookFileSize(t *testing.T) {
	notebookContent := []byte("# Databricks notebook source\nprint('cached')\n")
	modTime := time.Now()
	diskCache, err := filecache.NewDiskCache(t.TempDir(), 0, time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}
	if _, err := diskCache.Set("/test/notebook", notebookContent, modTime); err != nil {
		t.Fatalf("cache Set failed: %v", err)
	}

	n := &WSNode{
		wfClient:  &databricks.FakeWorkspaceAPI{},
		diskCache: diskCache,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeNotebook,
			Path:       "/test/notebook",
			Language:   workspace.LanguagePython,
			Size:       1,
			ModifiedAt: modTime.UnixMilli(),
		}},
	}

	if errno := n.ensureDataLocked(context.Background()); errno != 0 {
		t.Fatalf("ensureDataLocked failed: %d", errno)
	}
	if n.buf.CachedPath == "" {
		t.Fatal("expected cache path to be reused")
	}
	if n.buf.FileSize != int64(len(notebookContent)) {
		t.Fatalf("expected cache file size %d, got %d", len(notebookContent), n.buf.FileSize)
	}
	if n.fileInfo.Size() != int64(len(notebookContent)) {
		t.Fatalf("expected learned notebook size %d, got %d", len(notebookContent), n.fileInfo.Size())
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected cache-backed notebook size to be marked exact")
	}
}

func TestFlushNotebookPreservesExactSizeAfterStatFresh(t *testing.T) {
	notebookContent := []byte("print('hello')\n")
	cacheSetCalls := 0

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFreshFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path:       "/test/notebook",
				ObjectType: workspace.ObjectTypeNotebook,
				Language:   workspace.LanguagePython,
				Size:       1,
				ModifiedAt: time.Now().UnixMilli(),
			}}, nil
		},
		CacheSetFunc: func(path string, info fs.FileInfo) {
			cacheSetCalls++
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/notebook",
			ObjectType: workspace.ObjectTypeNotebook,
			Language:   workspace.LanguagePython,
			Size:       1,
			ModifiedAt: time.Now().Add(-time.Hour).UnixMilli(),
		}},
		buf: fileBuffer{Data: append([]byte(nil), notebookContent...), Dirty: true},
	}

	if errno := n.flushLocked(context.Background()); errno != 0 {
		t.Fatalf("flushLocked failed: %d", errno)
	}
	if n.fileInfo.Size() != int64(len(notebookContent)) {
		t.Fatalf("expected exact size %d after flush, got %d", len(notebookContent), n.fileInfo.Size())
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected notebook exact size after flush")
	}
	if cacheSetCalls == 0 {
		t.Fatal("expected exact notebook size to be published to cache")
	}
}

func TestFlushNotebookFallsBackToLocalExactSizeWhenStatFreshFails(t *testing.T) {
	notebookContent := []byte("print('hello')\n")

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFreshFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return nil, fs.ErrPermission
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/notebook",
			ObjectType: workspace.ObjectTypeNotebook,
			Language:   workspace.LanguagePython,
			Size:       1,
			ModifiedAt: time.Now().Add(-time.Hour).UnixMilli(),
		}},
		buf: fileBuffer{Data: append([]byte(nil), notebookContent...), Dirty: true},
	}

	if errno := n.flushLocked(context.Background()); errno != 0 {
		t.Fatalf("flushLocked failed: %d", errno)
	}
	if n.fileInfo.Size() != int64(len(notebookContent)) {
		t.Fatalf("expected local exact size %d after fallback, got %d", len(notebookContent), n.fileInfo.Size())
	}
	if !n.fileInfo.NotebookSizeComputed {
		t.Fatal("expected notebook exact size after fallback")
	}
}

func TestReadFallsBackToRemoteWhenCacheFileMissing(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte("fresh-data"), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       10,
		}},
		buf: fileBuffer{CachedPath: t.TempDir() + "/missing", FileSize: 10},
	}

	result, errno := n.Read(context.Background(), nil, make([]byte, 10), 0)
	if errno != 0 {
		t.Fatalf("Read failed: %d", errno)
	}
	data, _ := result.Bytes(nil)
	if string(data) != "fresh-data" {
		t.Fatalf("expected fallback data, got %q", string(data))
	}
	if n.buf.CachedPath != "" {
		t.Fatalf("expected cache path cleared after fallback, got %q", n.buf.CachedPath)
	}
}

func TestWriteFallsBackToRemoteWhenCacheChecksumMismatch(t *testing.T) {
	cacheFile := t.TempDir() + "/cache"
	if err := os.WriteFile(cacheFile, []byte("tampered"), 0600); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	readAllCalls := 0
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return []byte("remote"), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       6,
		}},
		buf: fileBuffer{
			CachedPath:     cacheFile,
			CachedChecksum: filecache.CalculateChecksum([]byte("original")),
			FileSize:       8,
		},
	}

	if written, errno := n.Write(context.Background(), nil, []byte("X"), 0); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	} else if written != 1 {
		t.Fatalf("expected 1 written byte, got %d", written)
	}
	if readAllCalls != 1 {
		t.Fatalf("expected one remote reload after checksum mismatch, got %d", readAllCalls)
	}
	if got := string(n.buf.Data); got != "Xemote" {
		t.Fatalf("unexpected buffer after write: %q", got)
	}
	if n.buf.CachedPath != "" || n.buf.CachedChecksum != "" {
		t.Fatalf("expected cached file metadata cleared, got path=%q checksum=%q", n.buf.CachedPath, n.buf.CachedChecksum)
	}
}

func TestWriteLoadsValidCacheFileForMutation(t *testing.T) {
	cacheFile := t.TempDir() + "/cache"
	if err := os.WriteFile(cacheFile, []byte("cached-data"), 0600); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       11,
		}},
		buf: fileBuffer{
			CachedPath:     cacheFile,
			CachedChecksum: filecache.CalculateChecksum([]byte("cached-data")),
			FileSize:       11,
		},
	}

	if written, errno := n.Write(context.Background(), nil, []byte("X"), 6); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	} else if written != 1 {
		t.Fatalf("expected 1 written byte, got %d", written)
	}
	if got := string(n.buf.Data); got != "cachedXdata" {
		t.Fatalf("unexpected buffer after write: %q", got)
	}
	if !n.buf.Dirty {
		t.Fatal("expected buffer to be dirty after write")
	}
}

func TestWriteFallsBackToRemoteWhenCacheFileMissing(t *testing.T) {
	readAllCalls := 0
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return []byte("remote"), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
			Size:       6,
		}},
		buf: fileBuffer{
			CachedPath: t.TempDir() + "/missing",
			FileSize:   6,
		},
	}

	if written, errno := n.Write(context.Background(), nil, []byte("X"), 0); errno != 0 {
		t.Fatalf("Write failed: %d", errno)
	} else if written != 1 {
		t.Fatalf("expected 1 written byte, got %d", written)
	}
	if readAllCalls != 1 {
		t.Fatalf("expected one remote reload after cache miss, got %d", readAllCalls)
	}
	if got := string(n.buf.Data); got != "Xemote" {
		t.Fatalf("unexpected buffer after write: %q", got)
	}
	if n.buf.CachedPath != "" || n.buf.CachedChecksum != "" {
		t.Fatalf("expected cached file metadata cleared, got path=%q checksum=%q", n.buf.CachedPath, n.buf.CachedChecksum)
	}
}

// ============================================================================
// Notebook Source Display Tests
// ============================================================================

func TestReaddirUsesSourceExtensionsForNotebooks(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadDirFunc: func(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
			return []fs.DirEntry{
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/notebook1",
					ObjectType: workspace.ObjectTypeNotebook,
					Language:   workspace.LanguagePython,
				}}},
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/file.txt",
					ObjectType: workspace.ObjectTypeFile,
				}}},
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/subdir",
					ObjectType: workspace.ObjectTypeDirectory,
				}}},
			}, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/test",
		}},
	}

	dirStream, errno := n.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir failed with errno: %d", errno)
	}

	entries := []fuse.DirEntry{}
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}

	// Check notebook has source extension
	found := false
	for _, e := range entries {
		if e.Name == "notebook1.py" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected notebook to have .py extension")
	}

	// Check regular file doesn't get notebook suffix
	for _, e := range entries {
		if e.Name == "file.txt.py" {
			t.Error("Regular file should not have notebook extension")
		}
	}

	// Check directory doesn't get notebook suffix
	for _, e := range entries {
		if e.Name == "subdir.py" {
			t.Error("Directory should not have notebook extension")
		}
	}
}

func TestReaddirFallsBackToIpynbOnCollision(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadDirFunc: func(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
			return []fs.DirEntry{
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/notebook1",
					ObjectType: workspace.ObjectTypeNotebook,
					Language:   workspace.LanguagePython,
				}}},
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/notebook1.py",
					ObjectType: workspace.ObjectTypeFile,
				}}},
			}, nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeDirectory,
			Path:       "/test",
		}},
	}

	dirStream, errno := n.Readdir(context.Background())
	if errno != 0 {
		t.Fatalf("Readdir failed with errno: %d", errno)
	}

	entries := []fuse.DirEntry{}
	for dirStream.HasNext() {
		entry, _ := dirStream.Next()
		entries = append(entries, entry)
	}

	if len(entries) != 2 {
		t.Fatalf("Expected 2 entries, got %d", len(entries))
	}

	foundFile := false
	foundNotebook := false
	for _, entry := range entries {
		if entry.Name == "notebook1.py" {
			foundFile = true
		}
		if entry.Name == "notebook1.ipynb" {
			foundNotebook = true
		}
	}
	if !foundFile || !foundNotebook {
		t.Fatalf("expected both exact file and fallback notebook entry, got %+v", entries)
	}
}

func TestValidateChildPath(t *testing.T) {
	tests := []struct {
		name       string
		parentPath string
		childName  string
		wantPath   string
		wantErr    bool
	}{
		{
			name:       "valid simple name",
			parentPath: "/Users/test",
			childName:  "file.txt",
			wantPath:   "/Users/test/file.txt",
			wantErr:    false,
		},
		{
			name:       "valid name with dots",
			parentPath: "/Users/test",
			childName:  "file.tar.gz",
			wantPath:   "/Users/test/file.tar.gz",
			wantErr:    false,
		},
		{
			name:       "reject dot",
			parentPath: "/Users/test",
			childName:  ".",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "reject dotdot",
			parentPath: "/Users/test",
			childName:  "..",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "reject path with slash",
			parentPath: "/Users/test",
			childName:  "subdir/file.txt",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "reject path traversal attempt",
			parentPath: "/Users/test",
			childName:  "../../../etc/passwd",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "reject backslash",
			parentPath: "/Users/test",
			childName:  "sub\\file.txt",
			wantPath:   "",
			wantErr:    true,
		},
		{
			name:       "valid hidden file",
			parentPath: "/Users/test",
			childName:  ".hidden",
			wantPath:   "/Users/test/.hidden",
			wantErr:    false,
		},
		{
			name:       "root parent",
			parentPath: "/",
			childName:  "file.txt",
			wantPath:   "/file.txt",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, err := validateChildPath(tt.parentPath, tt.childName)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateChildPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotPath != tt.wantPath {
				t.Errorf("validateChildPath() = %q, want %q", gotPath, tt.wantPath)
			}
		})
	}
}

// ============================================================================
// Cache Corruption Recovery Tests
// ============================================================================

// TestEnsureDataLockedWithMissingCacheFile verifies that ensureDataLocked
// re-fetches from remote when cache file is missing
func TestEnsureDataLockedWithMissingCacheFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a real disk cache
	cache, err := filecache.NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to create disk cache: %v", err)
	}

	originalData := []byte("original content from remote")
	remotePath := "/test/file.txt"
	modTime := time.Now()

	// Pre-populate cache with correct data
	localPath, err := cache.Set(remotePath, originalData, modTime)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Create the API mock that returns fresh data when called
	freshData := []byte("fresh content from remote")
	readAllCalled := false
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalled = true
			return freshData, nil
		},
	}

	// Create node with cache
	n := &WSNode{
		wfClient:  api,
		diskCache: cache,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       remotePath,
			Size:       int64(len(originalData)),
			ModifiedAt: modTime.UnixMilli(),
		}},
	}

	// Delete the cache file to simulate missing file
	if err := os.Remove(localPath); err != nil {
		t.Fatalf("Failed to remove cache file: %v", err)
	}

	// Call ensureDataLocked - should detect missing file and fetch from remote
	errno := n.ensureDataLocked(context.Background())
	if errno != 0 {
		t.Fatalf("ensureDataLocked failed with errno: %d", errno)
	}

	// Verify that ReadAll was called (recovery from missing cache)
	if !readAllCalled {
		t.Error("Expected ReadAll to be called after cache file missing")
	}

	// Verify that CachedPath is set (data fetched and cached)
	if n.buf.CachedPath == "" {
		t.Error("Expected CachedPath to be set after fetching from remote")
	}
}

// TestEnsureDataLockedWithValidCache verifies that ensureDataLocked sets
// CachedPath when cache is valid (on-demand read optimization)
func TestEnsureDataLockedWithValidCache(t *testing.T) {
	tmpDir := t.TempDir()

	// Create a real disk cache
	cache, err := filecache.NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to create disk cache: %v", err)
	}

	cachedData := []byte("cached content")
	remotePath := "/test/file.txt"
	modTime := time.Now()

	// Pre-populate cache
	localPath, err := cache.Set(remotePath, cachedData, modTime)
	if err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	// Create the API mock - should NOT be called
	readAllCalled := false
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalled = true
			return []byte("this should not be returned"), nil
		},
	}

	// Create node with cache
	n := &WSNode{
		wfClient:  api,
		diskCache: cache,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       remotePath,
			Size:       int64(len(cachedData)),
			ModifiedAt: modTime.UnixMilli(),
		}},
	}

	// Call ensureDataLocked - should use cache
	errno := n.ensureDataLocked(context.Background())
	if errno != 0 {
		t.Fatalf("ensureDataLocked failed with errno: %d", errno)
	}

	// Verify that ReadAll was NOT called (cache hit)
	if readAllCalled {
		t.Error("Expected ReadAll NOT to be called when cache is valid")
	}

	// Verify that CachedPath is set (on-demand read optimization)
	if n.buf.CachedPath != localPath {
		t.Errorf("Expected CachedPath to be %q, got %q", localPath, n.buf.CachedPath)
	}

	// Verify that Data is NOT loaded (lazy loading)
	if n.buf.Data != nil {
		t.Error("Expected Data to be nil (on-demand read optimization)")
	}

	// Verify that FileSize is set correctly
	if n.buf.FileSize != int64(len(cachedData)) {
		t.Errorf("Expected FileSize to be %d, got %d", len(cachedData), n.buf.FileSize)
	}
}

func TestReadUsesWarmDiskCacheWithoutRemoteRead(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := filecache.NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("Failed to create disk cache: %v", err)
	}

	cachedData := []byte("cached content")
	remotePath := "/test/file.txt"
	modTime := time.Now()

	if _, err := cache.Set(remotePath, cachedData, modTime); err != nil {
		t.Fatalf("Failed to set cache: %v", err)
	}

	readAllCalls := 0
	statCalls := 0
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			statCalls++
			return databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				ObjectType: workspace.ObjectTypeFile,
				Path:       remotePath,
				Size:       int64(len(cachedData)),
				ModifiedAt: modTime.UnixMilli(),
			}}, nil
		},
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			readAllCalls++
			return []byte("remote content"), nil
		},
	}

	n := &WSNode{
		wfClient:  api,
		diskCache: cache,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       remotePath,
			Size:       int64(len(cachedData)),
			ModifiedAt: modTime.UnixMilli(),
		}},
		metadataCheckedAt: time.Now(),
	}

	if _, _, errno := n.Open(context.Background(), 0); errno != 0 {
		t.Fatalf("Open failed: %d", errno)
	}

	for attempt := 0; attempt < 2; attempt++ {
		result, errno := n.Read(context.Background(), nil, make([]byte, len(cachedData)), 0)
		if errno != 0 {
			t.Fatalf("Read failed: %d", errno)
		}
		data, _ := result.Bytes(nil)
		if string(data) != string(cachedData) {
			t.Fatalf("expected cached data %q, got %q", string(cachedData), string(data))
		}
	}

	if readAllCalls != 0 {
		t.Fatalf("expected no remote ReadAll calls, got %d", readAllCalls)
	}
	if statCalls != 0 {
		t.Fatalf("expected no Stat calls within metadata TTL, got %d", statCalls)
	}
}
