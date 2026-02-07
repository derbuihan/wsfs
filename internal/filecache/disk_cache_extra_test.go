package filecache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEvictIfNeededCacheFull(t *testing.T) {
	cache := &DiskCache{
		maxSizeBytes: 1,
		ttl:          time.Hour,
		entries:      make(map[string]*Entry),
	}

	if err := cache.evictIfNeeded(10); err == nil {
		t.Fatal("expected cache full error")
	}
}

func TestEvictExpiredLocked(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "entry")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	cache := &DiskCache{
		cacheDir:     tmpDir,
		maxSizeBytes: 1024,
		ttl:          time.Second,
		entries:      make(map[string]*Entry),
	}
	entry := &Entry{
		RemotePath: "/file",
		LocalPath:  path,
		Size:       4,
		ModTime:    time.Now(),
		AccessTime: time.Now().Add(-2 * time.Second),
	}
	cache.entries["/file"] = entry
	cache.totalSize = entry.Size

	cache.mu.Lock()
	cache.evictExpiredLocked()
	cache.mu.Unlock()

	if _, ok := cache.entries["/file"]; ok {
		t.Fatal("expected entry to be evicted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file removed, err=%v", err)
	}
}

func TestCalculateFileChecksumError(t *testing.T) {
	if _, err := calculateFileChecksum("/does/not/exist"); err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyToCacheMissingSource(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	if _, err := cache.CopyToCache("/remote", filepath.Join(tmpDir, "missing"), time.Now()); err == nil {
		t.Fatal("expected error")
	}
}

func TestCopyToCacheDisabled(t *testing.T) {
	cache := NewDisabledCache()
	if _, err := cache.CopyToCache("/remote", "/tmp/missing", time.Now()); err == nil {
		t.Fatal("expected error for disabled cache")
	}
}
