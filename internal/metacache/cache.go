package metacache

import (
	"io/fs"
	"path"
	"sync"
	"time"
)

type negativeCacheEntry struct {
	fs.FileInfo
}

var negativeEntry fs.FileInfo = &negativeCacheEntry{}

type CacheEntry struct {
	info       fs.FileInfo
	expiration time.Time
}

type Cache struct {
	entries  map[string]*CacheEntry
	cacheTTL time.Duration
	mu       sync.Mutex
}

func NewCache(ttl time.Duration) *Cache {
	return &Cache{
		entries:  make(map[string]*CacheEntry),
		cacheTTL: ttl,
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

	var entryInfo fs.FileInfo
	if info == nil {
		entryInfo = negativeEntry
	} else {
		entryInfo = info
	}

	expiration := time.Now().Add(c.cacheTTL)
	c.entries[path] = &CacheEntry{info: entryInfo, expiration: expiration}
}

func (c *Cache) Invalidate(filePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.entries, filePath)
	delete(c.entries, path.Dir(filePath))
}
