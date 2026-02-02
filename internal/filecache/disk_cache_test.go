package filecache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewDiskCache(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	if cache.cacheDir != tmpDir {
		t.Errorf("Expected cacheDir %s, got %s", tmpDir, cache.cacheDir)
	}
	if cache.maxSizeBytes != 1024*1024 {
		t.Errorf("Expected maxSizeBytes 1048576, got %d", cache.maxSizeBytes)
	}
	if cache.ttl != 1*time.Hour {
		t.Errorf("Expected ttl 1h, got %v", cache.ttl)
	}

	// Check directory was created
	if _, err := os.Stat(tmpDir); os.IsNotExist(err) {
		t.Error("Cache directory was not created")
	}
}

func TestNewDiskCacheDefaults(t *testing.T) {
	tmpDir := t.TempDir()

	cache, err := NewDiskCache(tmpDir, 0, 0)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	if cache.maxSizeBytes != 10*1024*1024*1024 {
		t.Errorf("Expected default maxSizeBytes 10GB, got %d", cache.maxSizeBytes)
	}
	if cache.ttl != 24*time.Hour {
		t.Errorf("Expected default ttl 24h, got %v", cache.ttl)
	}
}

func TestNewDisabledCache(t *testing.T) {
	cache := NewDisabledCache()

	if !cache.IsDisabled() {
		t.Error("Expected cache to be disabled")
	}

	// Operations should not fail but should not cache anything
	_, _, found := cache.Get("/test", time.Now())
	if found {
		t.Error("Disabled cache should not return cached entries")
	}

	_, err := cache.Set("/test", []byte("data"), time.Now())
	if err == nil {
		t.Error("Expected error when setting in disabled cache")
	}
}

func TestDiskCacheBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Test Set and Get
	testData := []byte("Hello, World!")
	modTime := time.Now()
	remotePath := "/test.txt"

	localPath, err := cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify file was created
	if _, err := os.Stat(localPath); os.IsNotExist(err) {
		t.Error("Cache file was not created")
	}

	// Verify content
	content, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	if string(content) != string(testData) {
		t.Errorf("Expected content %q, got %q", string(testData), string(content))
	}

	// Test Get
	cachedPath, _, found := cache.Get(remotePath, modTime)
	if !found {
		t.Error("Expected cache hit")
	}
	if cachedPath != localPath {
		t.Errorf("Expected path %s, got %s", localPath, cachedPath)
	}

	// Test stats
	numEntries, totalSize := cache.GetStats()
	if numEntries != 1 {
		t.Errorf("Expected 1 entry, got %d", numEntries)
	}
	if totalSize != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), totalSize)
	}
}

func TestDiskCacheMiss(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Test cache miss
	_, _, found := cache.Get("/nonexistent.txt", time.Now())
	if found {
		t.Error("Expected cache miss for nonexistent file")
	}
}

func TestDiskCacheModTimeInvalidation(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := []byte("original")
	oldModTime := time.Now().Add(-1 * time.Hour)
	remotePath := "/test.txt"

	_, err = cache.Set(remotePath, testData, oldModTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Should get cache hit with same modTime
	_, _, found := cache.Get(remotePath, oldModTime)
	if !found {
		t.Error("Expected cache hit with same modTime")
	}

	// Should get cache miss with newer modTime (file was modified)
	newModTime := time.Now()
	_, _, found = cache.Get(remotePath, newModTime)
	if found {
		t.Error("Expected cache miss with newer modTime")
	}
}

func TestDiskCacheTTLExpiration(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := []byte("test")
	remotePath := "/test.txt"
	modTime := time.Now()

	_, err = cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Should find immediately
	_, _, found := cache.Get(remotePath, modTime)
	if !found {
		t.Error("Expected cache hit immediately")
	}

	// Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// Should not find after TTL
	_, _, found = cache.Get(remotePath, modTime)
	if found {
		t.Error("Expected cache miss after TTL expiration")
	}
}

func TestDiskCacheDelete(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := []byte("test")
	remotePath := "/test.txt"
	modTime := time.Now()

	localPath, err := cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Verify exists
	_, _, found := cache.Get(remotePath, modTime)
	if !found {
		t.Error("Expected cache hit before delete")
	}

	// Delete
	err = cache.Delete(remotePath)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, _, found = cache.Get(remotePath, modTime)
	if found {
		t.Error("Expected cache miss after delete")
	}

	// Verify file was removed
	if _, err := os.Stat(localPath); !os.IsNotExist(err) {
		t.Error("Cache file should have been deleted")
	}

	// Stats should be updated
	numEntries, totalSize := cache.GetStats()
	if numEntries != 0 {
		t.Errorf("Expected 0 entries, got %d", numEntries)
	}
	if totalSize != 0 {
		t.Errorf("Expected size 0, got %d", totalSize)
	}
}

func TestDiskCacheClear(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Add multiple entries
	for i := 0; i < 5; i++ {
		remotePath := filepath.Join("/dir", string(rune('a'+i))+".txt")
		testData := []byte("test data")
		_, err := cache.Set(remotePath, testData, time.Now())
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Verify entries exist
	numEntries, _ := cache.GetStats()
	if numEntries != 5 {
		t.Errorf("Expected 5 entries, got %d", numEntries)
	}

	// Clear
	err = cache.Clear()
	if err != nil {
		t.Fatalf("Clear failed: %v", err)
	}

	// Verify all cleared
	numEntries, totalSize := cache.GetStats()
	if numEntries != 0 {
		t.Errorf("Expected 0 entries after clear, got %d", numEntries)
	}
	if totalSize != 0 {
		t.Errorf("Expected size 0 after clear, got %d", totalSize)
	}
}

func TestDiskCacheLRUEviction(t *testing.T) {
	tmpDir := t.TempDir()
	// Small cache that can only hold 3 files
	cache, err := NewDiskCache(tmpDir, 30, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	modTime := time.Now()

	// Add 3 files (each 10 bytes)
	for i := 0; i < 3; i++ {
		remotePath := filepath.Join("/file", string(rune('a'+i))+".txt")
		testData := []byte("0123456789") // 10 bytes
		_, err := cache.Set(remotePath, testData, modTime)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different access times
	}

	// All 3 should be cached
	numEntries, totalSize := cache.GetStats()
	if numEntries != 3 {
		t.Errorf("Expected 3 entries, got %d", numEntries)
	}
	if totalSize != 30 {
		t.Errorf("Expected size 30, got %d", totalSize)
	}

	// Add 4th file - should evict oldest (file a)
	remotePath4 := "/file/d.txt"
	testData4 := []byte("0123456789")
	_, err = cache.Set(remotePath4, testData4, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Should still have 3 entries
	numEntries, totalSize = cache.GetStats()
	if numEntries != 3 {
		t.Errorf("Expected 3 entries after eviction, got %d", numEntries)
	}
	if totalSize != 30 {
		t.Errorf("Expected size 30 after eviction, got %d", totalSize)
	}

	// File 'a' should be evicted
	_, _, found := cache.Get("/file/a.txt", modTime)
	if found {
		t.Error("Expected oldest file to be evicted")
	}

	// Files b, c, d should still be cached
	_, _, found = cache.Get("/file/b.txt", modTime)
	if !found {
		t.Error("Expected file b to still be cached")
	}
	_, _, found = cache.Get("/file/c.txt", modTime)
	if !found {
		t.Error("Expected file c to still be cached")
	}
	_, _, found = cache.Get(remotePath4, modTime)
	if !found {
		t.Error("Expected new file d to be cached")
	}
}

func TestDiskCacheOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	remotePath := "/test.txt"
	modTime := time.Now()

	// Set original
	originalData := []byte("original")
	localPath1, err := cache.Set(remotePath, originalData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Overwrite
	newData := []byte("new content is longer")
	localPath2, err := cache.Set(remotePath, newData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Paths should be the same
	if localPath1 != localPath2 {
		t.Errorf("Expected same local path, got %s and %s", localPath1, localPath2)
	}

	// Content should be updated
	content, err := os.ReadFile(localPath2)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	if string(content) != string(newData) {
		t.Errorf("Expected content %q, got %q", string(newData), string(content))
	}

	// Size should be updated
	_, totalSize := cache.GetStats()
	if totalSize != int64(len(newData)) {
		t.Errorf("Expected size %d, got %d", len(newData), totalSize)
	}
}

func TestDiskCacheCopyToCache(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Create a temp file
	srcFile := filepath.Join(tmpDir, "source.txt")
	testData := []byte("test data for copy")
	if err := os.WriteFile(srcFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	remotePath := "/test.txt"
	modTime := time.Now()

	// Copy to cache
	localPath, err := cache.CopyToCache(remotePath, srcFile, modTime)
	if err != nil {
		t.Fatalf("CopyToCache failed: %v", err)
	}

	// Verify cached
	cachedPath, _, found := cache.Get(remotePath, modTime)
	if !found {
		t.Error("Expected cache hit after copy")
	}
	if cachedPath != localPath {
		t.Errorf("Expected path %s, got %s", localPath, cachedPath)
	}

	// Verify content
	content, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	if string(content) != string(testData) {
		t.Errorf("Expected content %q, got %q", string(testData), string(content))
	}
}

func TestDiskCacheGetCachedPaths(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Add files with delays to ensure different access times
	paths := []string{"/file/a.txt", "/file/b.txt", "/file/c.txt"}
	for _, path := range paths {
		_, err := cache.Set(path, []byte("data"), time.Now())
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Get cached paths (should be sorted by access time, oldest first)
	cachedPaths := cache.GetCachedPaths()
	if len(cachedPaths) != 3 {
		t.Errorf("Expected 3 cached paths, got %d", len(cachedPaths))
	}

	// Should be in order of access time
	for i, expectedPath := range paths {
		if cachedPaths[i] != expectedPath {
			t.Errorf("Expected path[%d] = %s, got %s", i, expectedPath, cachedPaths[i])
		}
	}
}

func TestLoadExistingEntries_CleansOrphanedFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some orphaned files in the cache directory
	orphanedFiles := []string{"abc123", "def456", "ghi789"}
	var totalOrphanedSize int64
	for i, name := range orphanedFiles {
		filePath := filepath.Join(tmpDir, name)
		data := make([]byte, (i+1)*100) // Different sizes
		if err := os.WriteFile(filePath, data, 0644); err != nil {
			t.Fatalf("Failed to create orphaned file: %v", err)
		}
		totalOrphanedSize += int64(len(data))
	}

	// Verify orphaned files exist
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("Expected 3 orphaned files, got %d", len(entries))
	}

	// Create a new cache - should clean up orphaned files
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Verify orphaned files were deleted
	entries, err = os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read dir: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Expected 0 files after cleanup, got %d", len(entries))
	}

	// Verify cache stats are consistent (should be empty)
	numEntries, totalSize := cache.GetStats()
	if numEntries != 0 {
		t.Errorf("Expected 0 entries, got %d", numEntries)
	}
	if totalSize != 0 {
		t.Errorf("Expected size 0, got %d", totalSize)
	}

	// Verify cache still works normally after cleanup
	testData := []byte("new data")
	remotePath := "/test.txt"
	modTime := time.Now()

	localPath, err := cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	cachedPath, _, found := cache.Get(remotePath, modTime)
	if !found {
		t.Error("Expected cache hit")
	}
	if cachedPath != localPath {
		t.Errorf("Expected path %s, got %s", localPath, cachedPath)
	}
}

func TestCalculateChecksum(t *testing.T) {
	// Test basic checksum calculation
	data := []byte("Hello, World!")
	checksum := CalculateChecksum(data)

	// SHA256 of "Hello, World!" should be consistent
	if len(checksum) != 64 { // SHA256 hex string is 64 characters
		t.Errorf("Expected checksum length 64, got %d", len(checksum))
	}

	// Same data should produce same checksum
	checksum2 := CalculateChecksum(data)
	if checksum != checksum2 {
		t.Error("Same data should produce same checksum")
	}

	// Different data should produce different checksum
	differentData := []byte("Hello, World!!")
	differentChecksum := CalculateChecksum(differentData)
	if checksum == differentChecksum {
		t.Error("Different data should produce different checksum")
	}

	// Empty data should have a valid checksum
	emptyChecksum := CalculateChecksum([]byte{})
	if len(emptyChecksum) != 64 {
		t.Errorf("Empty data checksum should be 64 chars, got %d", len(emptyChecksum))
	}
}

func TestDiskCacheChecksumVerification(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := []byte("test data for checksum verification")
	remotePath := "/checksum_test.txt"
	modTime := time.Now()

	// Set data
	localPath, err := cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get should return correct checksum
	cachedPath, checksum, found := cache.Get(remotePath, modTime)
	if !found {
		t.Fatal("Expected cache hit")
	}
	if cachedPath != localPath {
		t.Errorf("Expected path %s, got %s", localPath, cachedPath)
	}

	// Verify checksum matches what we'd calculate
	expectedChecksum := CalculateChecksum(testData)
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}

	// Read actual file and verify checksum
	actualData, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	actualChecksum := CalculateChecksum(actualData)
	if actualChecksum != checksum {
		t.Errorf("File checksum mismatch: expected %s, got %s", checksum, actualChecksum)
	}
}

func TestDiskCacheCopyToCacheChecksum(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	// Create a temp file
	srcFile := filepath.Join(tmpDir, "source.txt")
	testData := []byte("test data for CopyToCache checksum")
	if err := os.WriteFile(srcFile, testData, 0644); err != nil {
		t.Fatalf("Failed to create source file: %v", err)
	}

	remotePath := "/copy_checksum_test.txt"
	modTime := time.Now()

	// Copy to cache
	localPath, err := cache.CopyToCache(remotePath, srcFile, modTime)
	if err != nil {
		t.Fatalf("CopyToCache failed: %v", err)
	}

	// Get should return correct checksum
	cachedPath, checksum, found := cache.Get(remotePath, modTime)
	if !found {
		t.Fatal("Expected cache hit after copy")
	}
	if cachedPath != localPath {
		t.Errorf("Expected path %s, got %s", localPath, cachedPath)
	}

	// Verify checksum matches original data
	expectedChecksum := CalculateChecksum(testData)
	if checksum != expectedChecksum {
		t.Errorf("Expected checksum %s, got %s", expectedChecksum, checksum)
	}
}

func TestDiskCacheCorruptionDetection(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, 1*time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := []byte("original data")
	remotePath := "/corruption_test.txt"
	modTime := time.Now()

	// Set data
	localPath, err := cache.Set(remotePath, testData, modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get checksum before corruption
	_, originalChecksum, found := cache.Get(remotePath, modTime)
	if !found {
		t.Fatal("Expected cache hit")
	}

	// Corrupt the cache file
	corruptedData := []byte("corrupted!!")
	if err := os.WriteFile(localPath, corruptedData, 0600); err != nil {
		t.Fatalf("Failed to corrupt cache file: %v", err)
	}

	// Get should still return the entry (cache doesn't detect corruption on Get)
	// Corruption is detected by caller who verifies checksum
	_, checksum, found := cache.Get(remotePath, modTime)
	if !found {
		t.Fatal("Expected cache hit (Get doesn't verify content)")
	}

	// Checksum from Get should be the ORIGINAL checksum (from entry metadata)
	if checksum != originalChecksum {
		t.Error("Checksum in entry should not change when file is corrupted")
	}

	// Read the corrupted file and verify checksum mismatch
	actualData, err := os.ReadFile(localPath)
	if err != nil {
		t.Fatalf("Failed to read cache file: %v", err)
	}
	actualChecksum := CalculateChecksum(actualData)

	// The actual file checksum should NOT match the stored checksum
	if actualChecksum == checksum {
		t.Error("Corrupted file should have different checksum")
	}
}

func BenchmarkDiskCacheSet(b *testing.B) {
	tmpDir := b.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024*1024, 1*time.Hour)
	if err != nil {
		b.Fatalf("NewDiskCache failed: %v", err)
	}

	testData := make([]byte, 1024) // 1KB

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		remotePath := filepath.Join("/bench", string(rune(i%100))+".txt")
		_, err := cache.Set(remotePath, testData, time.Now())
		if err != nil {
			b.Fatalf("Set failed: %v", err)
		}
	}
}

func BenchmarkDiskCacheGet(b *testing.B) {
	tmpDir := b.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024*1024, 1*time.Hour)
	if err != nil {
		b.Fatalf("NewDiskCache failed: %v", err)
	}

	// Pre-populate
	remotePath := "/bench/test.txt"
	testData := make([]byte, 1024)
	modTime := time.Now()
	_, err = cache.Set(remotePath, testData, modTime)
	if err != nil {
		b.Fatalf("Set failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, found := cache.Get(remotePath, modTime)
		if !found {
			b.Fatal("Expected cache hit")
		}
	}
}
