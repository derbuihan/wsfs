package metacache

import (
	"io/fs"
	"path"
	"sync"
	"time"
)

// defaultMaxEntries is the default maximum number of entries in the cache
const defaultMaxEntries = 10000

type negativeCacheEntry struct {
	fs.FileInfo
}

var negativeEntry fs.FileInfo = &negativeCacheEntry{}

type CacheEntry struct {
	info       fs.FileInfo
	expiration time.Time
}

type Cache struct {
	entries    map[string]*CacheEntry
	cacheTTL   time.Duration
	maxEntries int
	mu         sync.Mutex
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries:    make(map[string]*CacheEntry),
		cacheTTL:   ttl,
		maxEntries: defaultMaxEntries,
	}
}

// NewCacheWithMaxEntries creates a cache with a custom max entries limit
func NewCacheWithMaxEntries(ttl time.Duration, maxEntries int) *Cache {
	if maxEntries <= 0 {
		maxEntries = defaultMaxEntries
	}
	return &Cache{
		entries:    make(map[string]*CacheEntry),
		cacheTTL:   ttl,
		maxEntries: maxEntries,
	}
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

	// Check if entry already exists (update case - no eviction needed)
	if _, exists := c.entries[path]; !exists {
		// Evict oldest entry if at capacity
		if len(c.entries) >= c.maxEntries {
			c.evictOldestLocked()
		}
	}

	var entryInfo fs.FileInfo
	if info == nil {
		entryInfo = negativeEntry
	} else {
		entryInfo = info
	}

	expiration := time.Now().Add(c.cacheTTL)
	c.entries[path] = &CacheEntry{info: entryInfo, expiration: expiration}
}

// evictOldestLocked removes the entry with the earliest expiration time
// Must be called with lock held
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

func (c *Cache) Invalidate(filePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, filePath)
	delete(c.entries, path.Dir(filePath))
}
