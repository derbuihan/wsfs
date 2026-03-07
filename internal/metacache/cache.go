package metacache

import (
	"io/fs"
	"path"
	"strings"
	"sync"
	"time"
)

// defaultMaxEntries is the default maximum number of direct path entries in the cache.
const defaultMaxEntries = 10000

type negativeCacheEntry struct {
	fs.FileInfo
}

var negativeEntry fs.FileInfo = &negativeCacheEntry{}

type CacheEntry struct {
	info       fs.FileInfo
	expiration time.Time
}

type DirLookupEntry struct {
	Name string
	Info fs.FileInfo
}

type dirCacheEntry struct {
	entries    []fs.DirEntry
	lookup     map[string]fs.FileInfo
	expiration time.Time
}

type Cache struct {
	entries     map[string]*CacheEntry
	dirEntries  map[string]*dirCacheEntry
	cacheTTL    time.Duration
	negativeTTL time.Duration
	maxEntries  int
	mu          sync.Mutex
}

func NewCache(ttl time.Duration) *Cache {
	return NewCacheWithConfig(ttl, ttl, defaultMaxEntries)
}

func NewCacheWithTTLs(ttl time.Duration, negativeTTL time.Duration) *Cache {
	return NewCacheWithConfig(ttl, negativeTTL, defaultMaxEntries)
}

// NewCacheWithMaxEntries creates a cache with a custom max entries limit.
func NewCacheWithMaxEntries(ttl time.Duration, maxEntries int) *Cache {
	return NewCacheWithConfig(ttl, ttl, maxEntries)
}

func NewCacheWithConfig(ttl time.Duration, negativeTTL time.Duration, maxEntries int) *Cache {
	if ttl <= 0 {
		ttl = time.Second
	}
	if negativeTTL <= 0 {
		negativeTTL = ttl
	}
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &Cache{
		entries:     make(map[string]*CacheEntry),
		dirEntries:  make(map[string]*dirCacheEntry),
		cacheTTL:    ttl,
		negativeTTL: negativeTTL,
		maxEntries:  maxEntries,
	}
}

func (c *Cache) PositiveTTL() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cacheTTL
}

func (c *Cache) Get(path string) (fs.FileInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, found := c.entries[path]
	if !found {
		return nil, false
	}

	if time.Now().After(entry.expiration) {
		delete(c.entries, path)
		return nil, false
	}

	if entry.info == negativeEntry {
		return nil, true
	}

	return entry.info, true
}

func (c *Cache) Set(path string, info fs.FileInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.setLocked(path, info)
}

func (c *Cache) setLocked(path string, info fs.FileInfo) {
	if _, exists := c.entries[path]; !exists && len(c.entries) >= c.maxEntries {
		c.evictOldestLocked()
	}

	expiration := time.Now().Add(c.cacheTTL)
	entryInfo := info
	if info == nil {
		expiration = time.Now().Add(c.negativeTTL)
		entryInfo = negativeEntry
	}
	c.entries[path] = &CacheEntry{info: entryInfo, expiration: expiration}
}

func (c *Cache) SetDirEntries(dirPath string, entries []fs.DirEntry, lookups []DirLookupEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry := &dirCacheEntry{
		entries:    cloneDirEntries(entries),
		lookup:     make(map[string]fs.FileInfo, len(lookups)),
		expiration: time.Now().Add(c.cacheTTL),
	}
	for _, lookup := range lookups {
		if lookup.Name == "" {
			continue
		}
		entry.lookup[lookup.Name] = lookup.Info
	}
	c.dirEntries[dirPath] = entry
}

func (c *Cache) GetDirEntries(dirPath string) ([]fs.DirEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, found := c.dirEntries[dirPath]
	if !found {
		return nil, false
	}
	if time.Now().After(entry.expiration) {
		delete(c.dirEntries, dirPath)
		return nil, false
	}
	return cloneDirEntries(entry.entries), true
}

// LookupDirEntry looks up a child by parent directory cache.
// If the parent directory cache is fresh, found is true. A nil info with found=true means
// the parent directory was cached and the child name was absent.
func (c *Cache) LookupDirEntry(filePath string) (fs.FileInfo, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	parent := path.Dir(filePath)
	name := path.Base(filePath)

	entry, found := c.dirEntries[parent]
	if !found {
		return nil, false
	}
	if time.Now().After(entry.expiration) {
		delete(c.dirEntries, parent)
		return nil, false
	}

	info, ok := entry.lookup[name]
	if !ok {
		return nil, true
	}
	return info, true
}

func (c *Cache) Invalidate(filePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.invalidateLocked(filePath)
}

func (c *Cache) invalidateLocked(filePath string) {
	delete(c.entries, filePath)
	delete(c.dirEntries, filePath)

	parent := path.Dir(filePath)
	delete(c.entries, parent)
	delete(c.dirEntries, parent)

	prefix := normalizedPrefix(filePath)
	for candidate := range c.entries {
		if strings.HasPrefix(candidate, prefix) {
			delete(c.entries, candidate)
		}
	}
	for candidate := range c.dirEntries {
		if strings.HasPrefix(candidate, prefix) {
			delete(c.dirEntries, candidate)
		}
	}
}

func normalizedPrefix(filePath string) string {
	if filePath == "/" {
		return "/"
	}
	return strings.TrimSuffix(filePath, "/") + "/"
}

// evictOldestLocked removes the direct path entry with the earliest expiration time.
// Must be called with lock held.
func (c *Cache) evictOldestLocked() {
	if len(c.entries) == 0 {
		return
	}

	var oldestPath string
	var oldestExp time.Time
	first := true

	for path, entry := range c.entries {
		if first || entry.expiration.Before(oldestExp) {
			oldestPath = path
			oldestExp = entry.expiration
			first = false
		}
	}

	if oldestPath != "" {
		delete(c.entries, oldestPath)
	}
}

func cloneDirEntries(entries []fs.DirEntry) []fs.DirEntry {
	if len(entries) == 0 {
		return nil
	}
	cloned := make([]fs.DirEntry, len(entries))
	copy(cloned, entries)
	return cloned
}
