package fuse

import (
	"context"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
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
	in.Valid = fuse.FATTR_SIZE
	in.Size = 5
	out := &fuse.AttrOut{}

	errno := n.Setattr(context.Background(), nil, in, out)
	if errno != 0 {
		t.Fatalf("Setattr failed with errno: %d", errno)
	}
	if out.Size != 5 {
		t.Errorf("Expected size 5, got %d", out.Size)
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

// TestWSNodeAccess tests that Access always returns 0 (allow all)
func TestWSNodeAccess(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test.txt",
		}},
	}

	// Test various access masks
	masks := []uint32{0, 1, 2, 4, 7}
	for _, mask := range masks {
		errno := n.Access(context.Background(), mask)
		if errno != 0 {
			t.Errorf("Access(mask=%d) returned errno %d, expected 0", mask, errno)
		}
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

// TestOpenDetectsRemoteModification verifies that Open() invalidates cache when remote file is modified
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
		buf: fileBuffer{Data: originalData, Dirty: false},
	}

	// Open should detect remote modification and invalidate cache
	_, _, errno := n.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	// After Open, the buffer should have new data (fetched via ReadAll)
	if string(n.buf.Data) != string(newData) {
		t.Errorf("Expected buffer to have new data %q, got %q", string(newData), string(n.buf.Data))
	}

	// ReadAll should have been called once to fetch new data after cache invalidation
	if readAllCallCount != 1 {
		t.Errorf("Expected ReadAll to be called once after cache invalidation, got %d", readAllCallCount)
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
		buf: fileBuffer{Data: localData, Dirty: true}, // Buffer is dirty
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
	readAllCalled := false

	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
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
		buf: fileBuffer{Data: originalData, Dirty: false},
	}

	_, _, errno := n.Open(context.Background(), 0)
	if errno != 0 {
		t.Fatalf("Open failed with errno: %d", errno)
	}

	// Buffer should still have original data
	if string(n.buf.Data) != string(originalData) {
		t.Error("Expected buffer to keep original data")
	}

	// ReadAll should not be called since remote is unchanged
	if readAllCalled {
		t.Error("Expected ReadAll not to be called when remote is unchanged")
	}
}

// ============================================================================
// Notebook (.ipynb) Extension Tests
// ============================================================================

// TestReaddirAddsIpynbExtension verifies that Readdir adds .ipynb extension to notebooks
func TestReaddirAddsIpynbExtension(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadDirFunc: func(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
			return []fs.DirEntry{
				databricks.WSDirEntry{WSFileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
					Path:       "/test/notebook1",
					ObjectType: workspace.ObjectTypeNotebook,
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

	// Check notebook has .ipynb extension
	found := false
	for _, e := range entries {
		if e.Name == "notebook1.ipynb" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected notebook to have .ipynb extension")
	}

	// Check regular file doesn't have .ipynb extension
	for _, e := range entries {
		if e.Name == "file.txt.ipynb" {
			t.Error("Regular file should not have .ipynb extension")
		}
	}

	// Check directory doesn't have .ipynb extension
	for _, e := range entries {
		if e.Name == "subdir.ipynb" {
			t.Error("Directory should not have .ipynb extension")
		}
	}
}

// Note: TestCreateNotebook is not included here because Create() requires
// a fully initialized FUSE bridge (NewPersistentInode). The notebook creation
// logic is tested via client_test.go::TestWriteNotebook instead.
