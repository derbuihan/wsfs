package fuse

import (
	"context"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/databricks"
)

// TestConcurrentReadWrite tests concurrent read and write operations on the same file.
// This verifies that the per-node mutex properly serializes access.
func TestConcurrentReadWrite(t *testing.T) {
	initialData := []byte("initial content for concurrent test")
	var writeCount atomic.Int32

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return initialData, nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCount.Add(1)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, int64(len(initialData)), false), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/concurrent_test.txt",
			Size:       int64(len(initialData)),
		}},
		buf: fileBuffer{Data: make([]byte, len(initialData))},
	}
	copy(n.buf.Data, initialData)

	const numGoroutines = 20
	const opsPerGoroutine = 50
	var wg sync.WaitGroup

	// Start concurrent readers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dest := make([]byte, 10)
			for j := 0; j < opsPerGoroutine; j++ {
				_, errno := n.Read(context.Background(), nil, dest, 0)
				if errno != 0 {
					t.Errorf("Read failed with errno: %d", errno)
				}
			}
		}()
	}

	// Start concurrent writers
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			data := []byte("write data")
			for j := 0; j < opsPerGoroutine; j++ {
				_, errno := n.Write(context.Background(), nil, data, int64(id*10))
				if errno != 0 {
					t.Errorf("Write failed with errno: %d", errno)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify buffer is still valid (not corrupted)
	if n.buf.Data == nil {
		t.Error("Buffer should not be nil after concurrent operations")
	}
}

// TestConcurrentTruncate tests rapid consecutive truncate operations.
// This verifies that truncate operations are properly serialized.
func TestConcurrentTruncate(t *testing.T) {
	initialData := make([]byte, 1000)
	for i := range initialData {
		initialData[i] = byte(i % 256)
	}

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 1000, false), nil
		},
		CacheSetFunc: func(path string, info fs.FileInfo) {},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/truncate_test.txt",
			Size:       int64(len(initialData)),
		}},
		buf: fileBuffer{Data: make([]byte, len(initialData))},
	}
	copy(n.buf.Data, initialData)

	const numGoroutines = 10
	var wg sync.WaitGroup

	// Start concurrent truncate operations with different sizes
	sizes := []uint64{500, 100, 800, 0, 200, 1500, 50, 300, 1000, 750}

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			n.mu.Lock()
			n.truncateLocked(sizes[idx%len(sizes)])
			n.mu.Unlock()
		}(i)
	}

	wg.Wait()

	// Verify buffer state is consistent
	n.mu.Lock()
	bufLen := len(n.buf.Data)
	bufSize := n.fileInfo.Size()
	n.mu.Unlock()

	if int64(bufLen) != bufSize {
		t.Errorf("Buffer length (%d) doesn't match fileInfo.Size (%d)", bufLen, bufSize)
	}
}

// TestConcurrentFlush tests multiple concurrent flush operations.
// This verifies that flush operations don't cause double writes or corruption.
func TestConcurrentFlush(t *testing.T) {
	var writeCount atomic.Int32
	var lastWrittenData []byte
	var dataMu sync.Mutex

	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCount.Add(1)
			dataMu.Lock()
			lastWrittenData = make([]byte, len(data))
			copy(lastWrittenData, data)
			dataMu.Unlock()
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			dataMu.Lock()
			size := len(lastWrittenData)
			dataMu.Unlock()
			return databricks.NewTestFileInfo(filePath, int64(size), false), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/flush_test.txt",
		}},
		buf: fileBuffer{Data: []byte("dirty content"), Dirty: true},
	}

	const numGoroutines = 10
	var wg sync.WaitGroup

	// Start concurrent flush operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errno := n.Flush(context.Background(), nil)
			if errno != 0 {
				t.Errorf("Flush failed with errno: %d", errno)
			}
		}()
	}

	wg.Wait()

	// Only one write should occur (first flush writes, subsequent flushes see Dirty=false)
	count := writeCount.Load()
	if count != 1 {
		t.Errorf("Expected exactly 1 write, got %d", count)
	}

	// Buffer should no longer be dirty
	if n.buf.Dirty {
		t.Error("Buffer should not be dirty after flush")
	}
}

// TestConcurrentOpenRead tests concurrent Open and Read operations.
func TestConcurrentOpenRead(t *testing.T) {
	testData := []byte("test data for concurrent open and read")

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return testData, nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, int64(len(testData)), false), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/open_read_test.txt",
			Size:       int64(len(testData)),
		}},
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	var openErrors atomic.Int32
	var readErrors atomic.Int32

	// Concurrent Open operations
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, errno := n.Open(context.Background(), 0)
			if errno != 0 {
				openErrors.Add(1)
			}
		}()
	}

	// Concurrent Read operations
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dest := make([]byte, 10)
			_, errno := n.Read(context.Background(), nil, dest, 0)
			if errno != 0 {
				readErrors.Add(1)
			}
		}()
	}

	wg.Wait()

	if openErrors.Load() > 0 {
		t.Errorf("Had %d Open errors", openErrors.Load())
	}
	if readErrors.Load() > 0 {
		t.Errorf("Had %d Read errors", readErrors.Load())
	}
}

// TestConcurrentWriteFlush tests concurrent Write and Flush operations.
func TestConcurrentWriteFlush(t *testing.T) {
	var writeCount atomic.Int32

	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte("initial"), nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			writeCount.Add(1)
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 100, false), nil
		},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/write_flush_test.txt",
			Size:       7,
		}},
		buf: fileBuffer{Data: []byte("initial")},
	}

	const numGoroutines = 20
	var wg sync.WaitGroup

	// Concurrent Write operations
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				data := []byte("new data")
				_, errno := n.Write(context.Background(), nil, data, int64(id))
				if errno != 0 {
					t.Errorf("Write failed with errno: %d", errno)
				}
			}
		}(i)
	}

	// Concurrent Flush operations
	for i := 0; i < numGoroutines/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				errno := n.Flush(context.Background(), nil)
				if errno != 0 {
					t.Errorf("Flush failed with errno: %d", errno)
				}
			}
		}()
	}

	wg.Wait()

	// Should have at least one write (may have more depending on timing)
	if writeCount.Load() == 0 {
		t.Error("Expected at least one write to occur")
	}
}

// TestRapidSetattrTruncate tests rapid Setattr truncate operations.
func TestRapidSetattrTruncate(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return make([]byte, 1000), nil
		},
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return nil
		},
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 1000, false), nil
		},
		CacheSetFunc: func(path string, info fs.FileInfo) {},
	}

	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/setattr_test.txt",
			Size:       1000,
		}},
		buf: fileBuffer{Data: make([]byte, 1000)},
	}

	// Rapid sequential truncates (as would happen in real use)
	sizes := []uint64{500, 100, 800, 0, 200, 1500, 50, 300, 1000, 750}

	for _, size := range sizes {
		n.mu.Lock()
		if size > 0 && n.buf.Data == nil {
			n.buf.Data = make([]byte, 0)
		}
		n.truncateLocked(size)
		n.mu.Unlock()
	}

	// Final state should be consistent
	n.mu.Lock()
	bufLen := len(n.buf.Data)
	bufSize := n.fileInfo.Size()
	n.mu.Unlock()

	if int64(bufLen) != bufSize {
		t.Errorf("After rapid truncates: buffer length (%d) != fileInfo.Size (%d)", bufLen, bufSize)
	}
}

// TestDirtyNodeRegistryConcurrent tests concurrent access to DirtyNodeRegistry.
func TestDirtyNodeRegistryConcurrent(t *testing.T) {
	registry := NewDirtyNodeRegistry()

	const numNodes = 100
	nodes := make([]*WSNode, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = &WSNode{
			fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
				Path: "/test_" + string(rune('a'+i%26)),
			}},
		}
	}

	var wg sync.WaitGroup

	// Concurrent Register operations
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			registry.Register(nodes[idx])
		}(i)
	}

	wg.Wait()

	// All nodes should be registered
	if registry.Count() != numNodes {
		t.Errorf("Expected %d nodes registered, got %d", numNodes, registry.Count())
	}

	// Concurrent Unregister operations
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			registry.Unregister(nodes[idx])
		}(i)
	}

	wg.Wait()

	// All nodes should be unregistered
	if registry.Count() != 0 {
		t.Errorf("Expected 0 nodes after unregister, got %d", registry.Count())
	}
}
