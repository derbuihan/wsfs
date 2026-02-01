package filecache

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Entry represents a cached file entry
type Entry struct {
	RemotePath string
	LocalPath  string
	Size       int64
	ModTime    time.Time
	AccessTime time.Time
}

// DiskCache manages on-disk file caching with LRU and TTL eviction
type DiskCache struct {
	cacheDir      string
	maxSizeBytes  int64
	ttl           time.Duration
	entries       map[string]*Entry // remotePath -> Entry
	totalSize     int64
	mu            sync.RWMutex
	disabled      bool
}

// NewDiskCache creates a new disk cache
// If maxSizeBytes is 0, defaults to 10GB
// If ttl is 0, defaults to 24 hours
func NewDiskCache(cacheDir string, maxSizeBytes int64, ttl time.Duration) (*DiskCache, error) {
	if maxSizeBytes == 0 {
		maxSizeBytes = 10 * 1024 * 1024 * 1024 // 10GB default
	}
	if ttl == 0 {
		ttl = 24 * time.Hour // 24 hours default
	}

	// Create cache directory if it doesn't exist
	// Use 0700 to prevent other users from reading cached files
	if err := os.MkdirAll(cacheDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %w", err)
	}

	cache := &DiskCache{
		cacheDir:     cacheDir,
		maxSizeBytes: maxSizeBytes,
		ttl:          ttl,
		entries:      make(map[string]*Entry),
		totalSize:    0,
	}

	// Load existing cache entries from disk
	if err := cache.loadExistingEntries(); err != nil {
		// Log error but don't fail - we can start with empty cache
		fmt.Fprintf(os.Stderr, "Warning: failed to load existing cache entries: %v\n", err)
	}

	return cache, nil
}

// NewDisabledCache creates a disabled cache (pass-through mode)
func NewDisabledCache() *DiskCache {
	return &DiskCache{
		disabled: true,
		entries:  make(map[string]*Entry),
	}
}

// IsDisabled returns true if cache is disabled
func (c *DiskCache) IsDisabled() bool {
	return c.disabled
}

// Get retrieves a cached file if it exists and is valid
// Returns localPath and true if cache hit, empty string and false if cache miss
// remoteModTime is used to validate cache freshness
func (c *DiskCache) Get(remotePath string, remoteModTime time.Time) (string, bool) {
	if c.disabled {
		return "", false
	}

	c.mu.RLock()
	entry, found := c.entries[remotePath]
	c.mu.RUnlock()

	if !found {
		return "", false
	}

	// Check TTL
	if time.Since(entry.AccessTime) > c.ttl {
		c.Delete(remotePath)
		return "", false
	}

	// Check if remote file was modified
	if !remoteModTime.IsZero() && remoteModTime.After(entry.ModTime) {
		c.Delete(remotePath)
		return "", false
	}

	// Check if local file still exists
	if _, err := os.Stat(entry.LocalPath); err != nil {
		c.Delete(remotePath)
		return "", false
	}

	// Update access time
	c.mu.Lock()
	entry.AccessTime = time.Now()
	c.mu.Unlock()

	return entry.LocalPath, true
}

// Set stores a file in the cache
// data is the file content to cache
// remoteModTime is the modification time from remote
func (c *DiskCache) Set(remotePath string, data []byte, remoteModTime time.Time) (string, error) {
	if c.disabled {
		return "", fmt.Errorf("cache is disabled")
	}

	size := int64(len(data))

	// Check if we need to evict entries
	if err := c.evictIfNeeded(size); err != nil {
		return "", fmt.Errorf("failed to evict entries: %w", err)
	}

	// Generate local path
	localPath := c.generateLocalPath(remotePath)

	// Write data to disk with restricted permissions (owner only)
	if err := os.WriteFile(localPath, data, 0600); err != nil {
		return "", fmt.Errorf("failed to write cache file: %w", err)
	}

	// Add entry
	now := time.Now()
	entry := &Entry{
		RemotePath: remotePath,
		LocalPath:  localPath,
		Size:       size,
		ModTime:    remoteModTime,
		AccessTime: now,
	}

	c.mu.Lock()
	// Remove old entry if exists
	if oldEntry, exists := c.entries[remotePath]; exists {
		c.totalSize -= oldEntry.Size
		// Only remove file if it's different from the new path
		if oldEntry.LocalPath != localPath {
			os.Remove(oldEntry.LocalPath) // Best effort cleanup
		}
	}
	c.entries[remotePath] = entry
	c.totalSize += size
	c.mu.Unlock()

	return localPath, nil
}

// Delete removes a file from the cache
func (c *DiskCache) Delete(remotePath string) error {
	if c.disabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, found := c.entries[remotePath]
	if !found {
		return nil
	}

	// Remove file
	os.Remove(entry.LocalPath) // Best effort

	// Remove entry
	delete(c.entries, remotePath)
	c.totalSize -= entry.Size

	return nil
}

// Clear removes all cached files
func (c *DiskCache) Clear() error {
	if c.disabled {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Remove all files
	for _, entry := range c.entries {
		os.Remove(entry.LocalPath) // Best effort
	}

	// Clear entries
	c.entries = make(map[string]*Entry)
	c.totalSize = 0

	return nil
}

// GetStats returns cache statistics
func (c *DiskCache) GetStats() (numEntries int, totalSize int64) {
	if c.disabled {
		return 0, 0
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries), c.totalSize
}

// evictIfNeeded evicts entries if necessary to make room for newSize bytes
func (c *DiskCache) evictIfNeeded(newSize int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, evict expired entries
	c.evictExpiredLocked()

	// If still over capacity, evict by LRU
	for c.totalSize+newSize > c.maxSizeBytes && len(c.entries) > 0 {
		if err := c.evictLRULocked(); err != nil {
			return err
		}
	}

	// If still can't fit, return error
	if c.totalSize+newSize > c.maxSizeBytes {
		return fmt.Errorf("cache full: cannot fit %d bytes (current: %d, max: %d)", newSize, c.totalSize, c.maxSizeBytes)
	}

	return nil
}

// evictExpiredLocked removes entries that have exceeded TTL
// Must be called with lock held
func (c *DiskCache) evictExpiredLocked() {
	now := time.Now()
	var toDelete []string

	for path, entry := range c.entries {
		if now.Sub(entry.AccessTime) > c.ttl {
			toDelete = append(toDelete, path)
		}
	}

	for _, path := range toDelete {
		entry := c.entries[path]
		os.Remove(entry.LocalPath) // Best effort
		delete(c.entries, path)
		c.totalSize -= entry.Size
	}
}

// evictLRULocked removes the least recently used entry
// Must be called with lock held
func (c *DiskCache) evictLRULocked() error {
	if len(c.entries) == 0 {
		return fmt.Errorf("no entries to evict")
	}

	// Find LRU entry
	var oldestPath string
	var oldestTime time.Time
	first := true

	for path, entry := range c.entries {
		if first || entry.AccessTime.Before(oldestTime) {
			oldestPath = path
			oldestTime = entry.AccessTime
			first = false
		}
	}

	// Remove oldest entry
	entry := c.entries[oldestPath]
	os.Remove(entry.LocalPath) // Best effort
	delete(c.entries, oldestPath)
	c.totalSize -= entry.Size

	return nil
}

// generateLocalPath generates a local file path for a remote path
func (c *DiskCache) generateLocalPath(remotePath string) string {
	// Use SHA256 hash to avoid path length issues and collisions
	hash := sha256.Sum256([]byte(remotePath))
	hashStr := hex.EncodeToString(hash[:])
	return filepath.Join(c.cacheDir, hashStr)
}

// loadExistingEntries scans the cache directory and loads existing cache files
func (c *DiskCache) loadExistingEntries() error {
	entries, err := os.ReadDir(c.cacheDir)
	if err != nil {
		return err
	}

	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fullPath := filepath.Join(c.cacheDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Check if file is expired
		if now.Sub(info.ModTime()) > c.ttl {
			os.Remove(fullPath) // Clean up expired file
			continue
		}

		// We can't recover the remote path from the hash, so we'll just track size
		// These entries will be overwritten when accessed
		c.totalSize += info.Size()
	}

	return nil
}

// CopyToCache copies a file from srcPath to cache for remotePath
// This is useful when we already have the data in a temp file
func (c *DiskCache) CopyToCache(remotePath string, srcPath string, remoteModTime time.Time) (string, error) {
	if c.disabled {
		return "", fmt.Errorf("cache is disabled")
	}

	// Get file size
	info, err := os.Stat(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat source file: %w", err)
	}
	size := info.Size()

	// Check if we need to evict entries
	if err := c.evictIfNeeded(size); err != nil {
		return "", fmt.Errorf("failed to evict entries: %w", err)
	}

	// Generate local path
	localPath := c.generateLocalPath(remotePath)

	// Copy file
	src, err := os.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("failed to open source file: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return "", fmt.Errorf("failed to create cache file: %w", err)
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		os.Remove(localPath) // Clean up on error
		return "", fmt.Errorf("failed to copy file: %w", err)
	}

	// Add entry
	now := time.Now()
	entry := &Entry{
		RemotePath: remotePath,
		LocalPath:  localPath,
		Size:       size,
		ModTime:    remoteModTime,
		AccessTime: now,
	}

	c.mu.Lock()
	// Remove old entry if exists
	if oldEntry, exists := c.entries[remotePath]; exists {
		c.totalSize -= oldEntry.Size
		// Only remove file if it's different from the new path
		if oldEntry.LocalPath != localPath {
			os.Remove(oldEntry.LocalPath) // Best effort cleanup
		}
	}
	c.entries[remotePath] = entry
	c.totalSize += size
	c.mu.Unlock()

	return localPath, nil
}

// GetCachedPaths returns all cached remote paths, sorted by access time (oldest first)
func (c *DiskCache) GetCachedPaths() []string {
	if c.disabled {
		return nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	type pathWithTime struct {
		path       string
		accessTime time.Time
	}

	paths := make([]pathWithTime, 0, len(c.entries))
	for path, entry := range c.entries {
		paths = append(paths, pathWithTime{path: path, accessTime: entry.AccessTime})
	}

	// Sort by access time (oldest first)
	sort.Slice(paths, func(i, j int) bool {
		return paths[i].accessTime.Before(paths[j].accessTime)
	})

	result := make([]string, len(paths))
	for i, p := range paths {
		result[i] = p.path
	}

	return result
}
