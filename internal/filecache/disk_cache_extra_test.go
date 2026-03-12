package filecache

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestResolveDefaultCacheDir(t *testing.T) {
	tests := []struct {
		name         string
		userCacheDir string
		userCacheErr error
		homeDir      string
		homeErr      error
		want         string
		wantErr      string
	}{
		{
			name:         "prefers user cache dir",
			userCacheDir: "/tmp/user-cache",
			homeDir:      "/home/tester",
			want:         "/tmp/user-cache/wsfs",
		},
		{
			name:    "falls back to home cache dir",
			homeDir: "/home/tester",
			want:    "/home/tester/.cache/wsfs",
		},
		{
			name:    "wraps home error when no cache dirs available",
			homeErr: errors.New("missing home"),
			wantErr: "resolve cache dir: missing home",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveDefaultCacheDir(tt.userCacheDir, tt.userCacheErr, tt.homeDir, tt.homeErr)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("resolveDefaultCacheDir() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveDefaultCacheDir() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveDefaultCacheDir() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDiskCacheCacheDir(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}
	if got := cache.CacheDir(); got != tmpDir {
		t.Fatalf("CacheDir() = %q, want %q", got, tmpDir)
	}
}

func TestDiskCacheGetMissingLocalFileInvalidatesEntryAndStats(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	modTime := time.Now()
	localPath, err := cache.Set("/missing-local.txt", []byte("payload"), modTime)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := os.Remove(localPath); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if _, _, found := cache.Get("/missing-local.txt", modTime); found {
		t.Fatal("expected cache miss after local file disappears")
	}

	numEntries, totalSize := cache.GetStats()
	if numEntries != 0 || totalSize != 0 {
		t.Fatalf("expected cache entry cleanup, got entries=%d size=%d", numEntries, totalSize)
	}
}

func TestDiskCacheCopyToCacheOverwriteUpdatesChecksumAndStats(t *testing.T) {
	tmpDir := t.TempDir()
	cache, err := NewDiskCache(tmpDir, 1024*1024, time.Hour)
	if err != nil {
		t.Fatalf("NewDiskCache failed: %v", err)
	}

	srcFile := filepath.Join(tmpDir, "source.txt")
	first := []byte("first")
	if err := os.WriteFile(srcFile, first, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	modTime := time.Now()
	localPath1, err := cache.CopyToCache("/copy.txt", srcFile, modTime)
	if err != nil {
		t.Fatalf("CopyToCache failed: %v", err)
	}

	second := []byte("second payload")
	if err := os.WriteFile(srcFile, second, 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	localPath2, err := cache.CopyToCache("/copy.txt", srcFile, modTime)
	if err != nil {
		t.Fatalf("CopyToCache overwrite failed: %v", err)
	}
	if localPath1 != localPath2 {
		t.Fatalf("expected stable local path, got %q and %q", localPath1, localPath2)
	}

	cachedPath, checksum, found := cache.Get("/copy.txt", modTime)
	if !found {
		t.Fatal("expected cache hit after overwrite")
	}
	if cachedPath != localPath2 {
		t.Fatalf("expected cached path %q, got %q", localPath2, cachedPath)
	}
	if checksum != CalculateChecksum(second) {
		t.Fatalf("expected checksum %q, got %q", CalculateChecksum(second), checksum)
	}

	numEntries, totalSize := cache.GetStats()
	if numEntries != 1 || totalSize != int64(len(second)) {
		t.Fatalf("unexpected cache stats after overwrite: entries=%d size=%d", numEntries, totalSize)
	}
}

func TestCopyFileToLocalCacheOpenFailure(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	if err := os.WriteFile(srcFile, []byte("payload"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}
	if err := os.Chmod(srcFile, 0); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}
	defer os.Chmod(srcFile, 0600)

	_, err := copyFileToLocalCache(srcFile, filepath.Join(tmpDir, "cache.txt"), calculateFileChecksum)
	if err == nil || !strings.Contains(err.Error(), "failed to open source file") {
		t.Fatalf("expected open failure, got %v", err)
	}
}

func TestCopyFileToLocalCacheCopyFailure(t *testing.T) {
	tmpDir := t.TempDir()
	srcDir := filepath.Join(tmpDir, "source-dir")
	if err := os.Mkdir(srcDir, 0700); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	_, err := copyFileToLocalCache(srcDir, filepath.Join(tmpDir, "cache.txt"), calculateFileChecksum)
	if err == nil || !strings.Contains(err.Error(), "failed to copy file") {
		t.Fatalf("expected copy failure, got %v", err)
	}
}

func TestCopyFileToLocalCacheChecksumFailure(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.txt")
	localPath := filepath.Join(tmpDir, "cache.txt")
	if err := os.WriteFile(srcFile, []byte("payload"), 0600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	checksumErr := errors.New("checksum failed")
	_, err := copyFileToLocalCache(srcFile, localPath, func(string) (string, error) {
		return "", checksumErr
	})
	if !errors.Is(err, checksumErr) {
		t.Fatalf("expected checksum error, got %v", err)
	}
	if _, statErr := os.Stat(localPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected local cache file cleanup, got %v", statErr)
	}
}
