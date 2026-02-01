package fuse

import (
	"context"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/buffer"
	"wsfs/internal/databricks"
)

func TestWSNodeTruncateLockedShrinks(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Size:       10,
		}},
		buf: buffer.FileBuffer{Data: []byte("0123456789")},
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
		buf: buffer.FileBuffer{Data: []byte("Hello, World!"), Dirty: false},
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
		buf: buffer.FileBuffer{Data: []byte{}},
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
		buf: buffer.FileBuffer{Data: []byte("test"), Dirty: false},
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
		buf: buffer.FileBuffer{Data: []byte("new content"), Dirty: true},
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
		buf: buffer.FileBuffer{Data: []byte("content"), Dirty: true},
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
		buf: buffer.FileBuffer{Data: []byte("synced content"), Dirty: true},
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
