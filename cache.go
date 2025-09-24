package main

import (
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
)

type CacheEntry struct {
	Info      *workspace.ObjectInfo
	Timestamp time.Time
}

var cache = sync.Map{}

const cacheTTL = 30 * time.Second

func getCachedObjectInfo(path string) (*workspace.ObjectInfo, bool) {
	if val, ok := cache.Load(path); ok {
		entry := val.(CacheEntry)
		if time.Since(entry.Timestamp) < cacheTTL {
			return entry.Info, true
		}
		cache.Delete(path)
	}
	return nil, false
}

func setCachedObjectInfo(path string, info *workspace.ObjectInfo) {
	cache.Store(path, CacheEntry{Info: info, Timestamp: time.Now()})
}
