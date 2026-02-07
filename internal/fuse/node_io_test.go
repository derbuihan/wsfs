package fuse

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestReadFromCacheFile(t *testing.T) {
	data := []byte("cached-data")
	path := filepath.Join(t.TempDir(), "cache")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	n := &WSNode{buf: fileBuffer{CachedPath: path, FileSize: int64(len(data))}}
	result, errno := n.readFromCacheFile(make([]byte, 6), 0)
	if errno != 0 {
		t.Fatalf("expected success, got %d", errno)
	}
	got, _ := result.Bytes(nil)
	if string(got) != string(data[:6]) {
		t.Fatalf("unexpected data: %q", string(got))
	}
}

func TestReadFromCacheFileMissing(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	n := &WSNode{buf: fileBuffer{CachedPath: missing, FileSize: 10}}
	_, errno := n.readFromCacheFile(make([]byte, 4), 0)
	if errno != syscall.EIO {
		t.Fatalf("expected EIO, got %d", errno)
	}
	if n.buf.CachedPath != "" {
		t.Fatalf("expected CachedPath cleared, got %q", n.buf.CachedPath)
	}
	if n.buf.FileSize != 0 {
		t.Fatalf("expected FileSize reset, got %d", n.buf.FileSize)
	}
}
