package metacache

import (
	"io/fs"
	"sync"
	"testing"
	"time"
)

// mockFileInfo is a simple implementation of fs.FileInfo for testing
type mockFileInfo struct {
	name    string
	size    int64
	mode    fs.FileMode
	modTime time.Time
	isDir   bool
}

func (m *mockFileInfo) Name() string       { return m.name }
func (m *mockFileInfo) Size() int64        { return m.size }
func (m *mockFileInfo) Mode() fs.FileMode  { return m.mode }
func (m *mockFileInfo) ModTime() time.Time { return m.modTime }
func (m *mockFileInfo) IsDir() bool        { return m.isDir }
func (m *mockFileInfo) Sys() any           { return nil }

func newMockFileInfo(name string, size int64, isDir bool) *mockFileInfo {
	mode := fs.FileMode(0644)
	if isDir {
		mode = fs.ModeDir | 0755
	}
	return &mockFileInfo{
		name:    name,
		size:    size,
		mode:    mode,
		modTime: time.Now(),
		isDir:   isDir,
	}
}

// TestCacheBasicOperations tests Set and Get operations
func TestCacheBasicOperations(t *testing.T) {
	c := NewCache(10 * time.Second)

	// Test Get on empty cache
	info, found := c.Get("/test.txt")
	if found {
		t.Error("Expected not found for empty cache")
	}
	if info != nil {
		t.Error("Expected nil info for empty cache")
	}

	// Test Set and Get
	testInfo := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", testInfo)

	info, found = c.Get("/test.txt")
	if !found {
		t.Error("Expected to find cached entry")
	}
	if info == nil {
		t.Fatal("Expected non-nil info")
	}
	if info.Name() != "test.txt" {
		t.Errorf("Expected name 'test.txt', got %q", info.Name())
	}
	if info.Size() != 100 {
		t.Errorf("Expected size 100, got %d", info.Size())
	}
}

// TestCacheExpiration tests that entries expire after TTL
func TestCacheExpiration(t *testing.T) {
	c := NewCache(100 * time.Millisecond)

	testInfo := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", testInfo)

	// Should be found immediately
	info, found := c.Get("/test.txt")
	if !found {
		t.Error("Expected to find cached entry immediately")
	}
	if info == nil {
		t.Fatal("Expected non-nil info")
	}

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)

	// Should not be found after expiration
	info, found = c.Get("/test.txt")
	if found {
		t.Error("Expected entry to be expired")
	}
	if info != nil {
		t.Error("Expected nil info after expiration")
	}
}

// TestCacheNegativeEntry tests caching of non-existent files
func TestCacheNegativeEntry(t *testing.T) {
	c := NewCache(10 * time.Second)

	// Set negative entry (nil info)
	c.Set("/nonexistent.txt", nil)

	// Should be found but return nil
	info, found := c.Get("/nonexistent.txt")
	if !found {
		t.Error("Expected to find negative cache entry")
	}
	if info != nil {
		t.Error("Expected nil info for negative entry")
	}
}

// TestCacheInvalidate tests cache invalidation
func TestCacheInvalidate(t *testing.T) {
	c := NewCache(10 * time.Second)

	// Set entries for file and parent directory
	fileInfo := newMockFileInfo("test.txt", 100, false)
	dirInfo := newMockFileInfo("dir", 0, true)

	c.Set("/dir/test.txt", fileInfo)
	c.Set("/dir", dirInfo)

	// Verify they exist
	_, found := c.Get("/dir/test.txt")
	if !found {
		t.Error("Expected to find file entry")
	}
	_, found = c.Get("/dir")
	if !found {
		t.Error("Expected to find dir entry")
	}

	// Invalidate the file
	c.Invalidate("/dir/test.txt")

	// File should be gone
	_, found = c.Get("/dir/test.txt")
	if found {
		t.Error("Expected file entry to be invalidated")
	}

	// Parent directory should also be gone
	_, found = c.Get("/dir")
	if found {
		t.Error("Expected parent dir entry to be invalidated")
	}
}

// TestCacheInvalidateRoot tests invalidation of root-level files
func TestCacheInvalidateRoot(t *testing.T) {
	c := NewCache(10 * time.Second)

	fileInfo := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", fileInfo)

	// Verify it exists
	_, found := c.Get("/test.txt")
	if !found {
		t.Error("Expected to find file entry")
	}

	// Invalidate
	c.Invalidate("/test.txt")

	// Should be gone
	_, found = c.Get("/test.txt")
	if found {
		t.Error("Expected file entry to be invalidated")
	}
}

// TestCacheOverwrite tests that Set overwrites existing entries
func TestCacheOverwrite(t *testing.T) {
	c := NewCache(10 * time.Second)

	// Set initial entry
	info1 := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", info1)

	// Verify
	info, found := c.Get("/test.txt")
	if !found || info.Size() != 100 {
		t.Error("Expected to find initial entry with size 100")
	}

	// Overwrite with different size
	info2 := newMockFileInfo("test.txt", 200, false)
	c.Set("/test.txt", info2)

	// Verify updated
	info, found = c.Get("/test.txt")
	if !found {
		t.Error("Expected to find updated entry")
	}
	if info.Size() != 200 {
		t.Errorf("Expected size 200, got %d", info.Size())
	}
}

// TestCacheConcurrency tests concurrent access to cache
func TestCacheConcurrency(t *testing.T) {
	c := NewCache(10 * time.Second)
	const numGoroutines = 100
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // readers and writers

	// Writers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := "/file" + string(rune(id%10))
				info := newMockFileInfo(path, int64(j), false)
				c.Set(path, info)
			}
		}(i)
	}

	// Readers
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				path := "/file" + string(rune(id%10))
				c.Get(path)
			}
		}(i)
	}

	wg.Wait()
	// If we get here without deadlock or race, test passes
}

// TestCacheMultipleFiles tests caching multiple files
func TestCacheMultipleFiles(t *testing.T) {
	c := NewCache(10 * time.Second)

	files := []string{"/a.txt", "/b.txt", "/c.txt", "/dir/d.txt"}
	for i, path := range files {
		info := newMockFileInfo(path, int64(i*100), false)
		c.Set(path, info)
	}

	// Verify all files are cached
	for i, path := range files {
		info, found := c.Get(path)
		if !found {
			t.Errorf("Expected to find %s", path)
		}
		if info == nil {
			t.Errorf("Expected non-nil info for %s", path)
			continue
		}
		if info.Size() != int64(i*100) {
			t.Errorf("Expected size %d for %s, got %d", i*100, path, info.Size())
		}
	}
}

// TestCacheDirectoryEntry tests caching directory entries
func TestCacheDirectoryEntry(t *testing.T) {
	c := NewCache(10 * time.Second)

	dirInfo := newMockFileInfo("mydir", 0, true)
	c.Set("/mydir", dirInfo)

	info, found := c.Get("/mydir")
	if !found {
		t.Error("Expected to find directory entry")
	}
	if !info.IsDir() {
		t.Error("Expected entry to be a directory")
	}
}

// TestCacheTTLUpdate tests that Set updates the TTL
func TestCacheTTLUpdate(t *testing.T) {
	c := NewCache(200 * time.Millisecond)

	info1 := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", info1)

	// Wait almost until expiration
	time.Sleep(150 * time.Millisecond)

	// Update the entry (resets TTL)
	info2 := newMockFileInfo("test.txt", 200, false)
	c.Set("/test.txt", info2)

	// Wait a bit more (past original expiration)
	time.Sleep(100 * time.Millisecond)

	// Should still be found (TTL was reset)
	info, found := c.Get("/test.txt")
	if !found {
		t.Error("Expected to find entry with reset TTL")
	}
	if info.Size() != 200 {
		t.Errorf("Expected size 200, got %d", info.Size())
	}
}

// TestCacheMaxEntries tests that cache evicts oldest entries when at capacity
func TestCacheMaxEntries(t *testing.T) {
	// Create cache with max 5 entries
	c := NewCacheWithMaxEntries(10*time.Second, 5)

	// Add 5 entries with staggered expiration times
	for i := 0; i < 5; i++ {
		path := "/file_" + string(rune('a'+i)) + ".txt"
		info := newMockFileInfo(path, int64(i*100), false)
		c.Set(path, info)
		time.Sleep(10 * time.Millisecond) // Ensure different expiration times
	}

	// Verify all 5 entries exist
	for i := 0; i < 5; i++ {
		path := "/file_" + string(rune('a'+i)) + ".txt"
		_, found := c.Get(path)
		if !found {
			t.Errorf("Expected to find %s before eviction", path)
		}
	}

	// Add 6th entry - should evict the oldest (/file_a.txt)
	info6 := newMockFileInfo("f.txt", 500, false)
	c.Set("/file_f.txt", info6)

	// /file_a.txt should be evicted (oldest expiration)
	_, found := c.Get("/file_a.txt")
	if found {
		t.Error("Expected oldest entry /file_a.txt to be evicted")
	}

	// Other entries should still exist
	for i := 1; i < 5; i++ {
		path := "/file_" + string(rune('a'+i)) + ".txt"
		_, found := c.Get(path)
		if !found {
			t.Errorf("Expected %s to still exist after eviction", path)
		}
	}

	// New entry should exist
	_, found = c.Get("/file_f.txt")
	if !found {
		t.Error("Expected new entry /file_f.txt to exist")
	}
}

// TestCacheMaxEntriesUpdate tests that updating existing entry doesn't trigger eviction
func TestCacheMaxEntriesUpdate(t *testing.T) {
	// Create cache with max 3 entries
	c := NewCacheWithMaxEntries(10*time.Second, 3)

	// Add 3 entries
	for i := 0; i < 3; i++ {
		path := "/file_" + string(rune('a'+i)) + ".txt"
		info := newMockFileInfo(path, int64(i*100), false)
		c.Set(path, info)
	}

	// Update existing entry - should NOT evict anything
	updatedInfo := newMockFileInfo("a.txt", 999, false)
	c.Set("/file_a.txt", updatedInfo)

	// All entries should still exist
	for i := 0; i < 3; i++ {
		path := "/file_" + string(rune('a'+i)) + ".txt"
		_, found := c.Get(path)
		if !found {
			t.Errorf("Expected %s to still exist after update", path)
		}
	}
}

// TestNewCacheWithMaxEntriesDefaults tests that invalid maxEntries uses default
func TestNewCacheWithMaxEntriesDefaults(t *testing.T) {
	// Zero should use default
	c1 := NewCacheWithMaxEntries(10*time.Second, 0)
	if c1.maxEntries != defaultMaxEntries {
		t.Errorf("Expected default maxEntries %d, got %d", defaultMaxEntries, c1.maxEntries)
	}

	// Negative should use default
	c2 := NewCacheWithMaxEntries(10*time.Second, -10)
	if c2.maxEntries != defaultMaxEntries {
		t.Errorf("Expected default maxEntries %d, got %d", defaultMaxEntries, c2.maxEntries)
	}

	// Positive should use the provided value
	c3 := NewCacheWithMaxEntries(10*time.Second, 100)
	if c3.maxEntries != 100 {
		t.Errorf("Expected maxEntries 100, got %d", c3.maxEntries)
	}
}

// BenchmarkCacheGet benchmarks cache Get operations
func BenchmarkCacheGet(b *testing.B) {
	c := NewCache(10 * time.Second)
	info := newMockFileInfo("test.txt", 100, false)
	c.Set("/test.txt", info)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("/test.txt")
	}
}

// BenchmarkCacheSet benchmarks cache Set operations
func BenchmarkCacheSet(b *testing.B) {
	c := NewCache(10 * time.Second)
	info := newMockFileInfo("test.txt", 100, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("/test.txt", info)
	}
}

// BenchmarkCacheGetMiss benchmarks cache misses
func BenchmarkCacheGetMiss(b *testing.B) {
	c := NewCache(10 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("/nonexistent.txt")
	}
}

// BenchmarkCacheInvalidate benchmarks cache invalidation
func BenchmarkCacheInvalidate(b *testing.B) {
	c := NewCache(10 * time.Second)
	info := newMockFileInfo("test.txt", 100, false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set("/dir/test.txt", info)
		c.Invalidate("/dir/test.txt")
	}
}
