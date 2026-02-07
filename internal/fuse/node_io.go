package fuse

import (
	"context"
	"errors"
	"io"
	iofs "io/fs"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/logging"
	"wsfs/internal/pathutil"
)

func (n *WSNode) ensureDataLocked(ctx context.Context) syscall.Errno {
	// If dirty, data must already be in memory
	if n.isDirtyLocked() {
		return 0
	}

	// If data is already in memory, nothing to do
	if n.buf.Data != nil {
		return 0
	}

	// If cache path is already set, nothing to do
	if n.buf.CachedPath != "" {
		return 0
	}

	if n.fileInfo.IsDir() {
		return syscall.EISDIR
	}

	remotePath := n.Path()
	remoteModTime := n.fileInfo.ModTime()

	// Try to get from cache first (only set CachedPath, don't load data)
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		cachedPath, _, found := n.diskCache.Get(remotePath, remoteModTime)
		if found {
			// Verify cache file exists
			if _, err := os.Stat(cachedPath); err == nil {
				n.buf.CachedPath = cachedPath
				n.buf.FileSize = n.fileInfo.Size()
				logging.Debugf("Cache path set for %s (on-demand read)", remotePath)
				return 0
			}
			// Cache file missing, delete entry and fall through to remote read
			logging.Debugf("Cache file missing for %s, fetching from remote", remotePath)
			n.diskCache.Delete(remotePath)
		}
	}

	// Cache miss or disabled - read from remote
	logging.Debugf("Cache miss for %s, fetching from remote", remotePath)
	readCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()
	data, err := n.wfClient.ReadAll(readCtx, remotePath)
	if err != nil {
		logging.Debugf("Failed to read file %s: %v", remotePath, err)
		if errors.Is(err, iofs.ErrNotExist) {
			return syscall.ENOENT
		}
		return syscall.EIO
	}

	// Store in cache and use cache path for on-demand reads
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		localPath, err := n.diskCache.Set(remotePath, data, remoteModTime)
		if err == nil {
			n.buf.CachedPath = localPath
			n.buf.FileSize = int64(len(data))
			logging.Debugf("Cached file %s (%d bytes), using on-demand read", remotePath, len(data))
			return 0
		}
		// Cache set failed, fall back to memory
		logging.Debugf("Failed to cache file %s: %v, using memory", remotePath, err)
	}

	// Fallback: keep data in memory (when cache is disabled or failed)
	n.buf.Data = data
	return 0
}

func (n *WSNode) truncateLocked(size uint64) {
	if size == 0 {
		n.buf.Data = []byte{}
	} else {
		// If data is in cache but not memory, load it first
		if n.buf.Data == nil && n.buf.CachedPath != "" {
			cacheData, err := os.ReadFile(n.buf.CachedPath)
			if err != nil {
				logging.Warnf("Failed to load cache file for truncate %s: %v", n.buf.CachedPath, err)
				// Fall through with empty data
				n.buf.Data = []byte{}
			} else {
				n.buf.Data = cacheData
			}
		}

		if n.buf.Data == nil {
			n.buf.Data = []byte{}
		}
		cur := uint64(len(n.buf.Data))
		if cur > size {
			n.buf.Data = n.buf.Data[:size]
		} else if cur < size {
			newData := make([]byte, size)
			copy(newData, n.buf.Data)
			n.buf.Data = newData
		}
	}

	n.fileInfo.ObjectInfo.Size = int64(size)
	n.pendingTruncate = true
	n.markDirtyLocked(dirtyTruncate)
}

func (n *WSNode) flushLocked(ctx context.Context) syscall.Errno {
	if !n.isDirtyLocked() || n.buf.Data == nil {
		return 0
	}

	// Apply timeout for write and stat operations
	opCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()

	remotePath := n.Path()
	err := n.wfClient.Write(opCtx, remotePath, n.buf.Data)
	if err != nil {
		logging.Warnf("Error writing back on Flush for %s: %v", remotePath, err)
		return syscall.EIO
	}
	n.clearDirtyLocked()

	info, err := n.wfClient.Stat(opCtx, remotePath)
	if err != nil {
		logging.Warnf("Error refreshing file info after Flush for %s: %v", remotePath, err)
		return 0
	}
	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		logging.Warnf("Unexpected file info type after Flush for %s", remotePath)
		return 0
	}
	n.fileInfo = wsInfo

	// Update cache with new content
	if n.diskCache != nil && !n.diskCache.IsDisabled() && n.buf.Data != nil {
		_, err := n.diskCache.Set(remotePath, n.buf.Data, n.fileInfo.ModTime())
		if err != nil {
			logging.Debugf("Failed to update cache after flush for %s: %v", remotePath, err)
		} else {
			logging.Debugf("Updated cache after flush for %s", remotePath)
		}
	}

	return 0
}

func (n *WSNode) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Open called on path: %s", n.fileInfo.Path)

	if n.fileInfo.IsDir() {
		return nil, 0, syscall.EISDIR
	}

	// Check for remote modifications before using cached data
	if (n.buf.Data != nil || n.buf.CachedPath != "") && !n.isDirtyLocked() {
		info, err := n.wfClient.Stat(ctx, n.fileInfo.Path)
		if err == nil {
			wsInfo, ok := info.(databricks.WSFileInfo)
			if ok && wsInfo.ModTime().After(n.fileInfo.ModTime()) {
				// Remote file was modified, invalidate cache
				logging.Debugf("Remote file modified, invalidating cache for %s", n.fileInfo.Path)
				n.buf.Data = nil
				n.buf.CachedPath = ""
				n.buf.FileSize = 0
				n.fileInfo = wsInfo
				// Also invalidate disk cache
				if n.diskCache != nil && !n.diskCache.IsDisabled() {
					actualPath := pathutil.ToRemotePath(n.fileInfo.Path)
					n.diskCache.Delete(actualPath)
				}
			}
		}
	}

	if flags&syscall.O_TRUNC != 0 {
		// Invalidate caches immediately for truncation
		n.wfClient.CacheInvalidate(n.Path())
		if n.diskCache != nil && !n.diskCache.IsDisabled() {
			actualPath := pathutil.ToRemotePath(n.fileInfo.Path)
			if err := n.diskCache.Delete(actualPath); err != nil {
				logging.Debugf("Failed to delete cache for %s: %v", actualPath, err)
			}
		}

		n.buf.CachedPath = ""
		n.buf.FileSize = 0
		n.truncateLocked(0)
		n.markModifiedLocked(time.Now())
	} else if n.buf.Data == nil && n.buf.CachedPath == "" {
		if errno := n.ensureDataLocked(ctx); errno != 0 {
			return nil, 0, errno
		}
	}

	openFlags := uint32(0)
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0 {
		openFlags |= fuse.FOPEN_DIRECT_IO
	} else {
		openFlags |= fuse.FOPEN_KEEP_CACHE
	}

	n.incrementOpenLocked()

	return nil, openFlags, 0
}

func (n *WSNode) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Read called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(dest))

	// 1. If dirty, must read from memory buffer
	if n.isDirtyLocked() && n.buf.Data != nil {
		return n.readFromMemory(dest, off)
	}

	// 2. If cache path is set, read directly from cache file (on-demand)
	if n.buf.CachedPath != "" {
		return n.readFromCacheFile(dest, off)
	}

	// 3. If data is in memory, read from memory
	if n.buf.Data != nil {
		return n.readFromMemory(dest, off)
	}

	// 4. Data not loaded yet, load it
	if errno := n.ensureDataLocked(ctx); errno != 0 {
		return nil, errno
	}

	// After ensureDataLocked, check again
	if n.buf.CachedPath != "" {
		return n.readFromCacheFile(dest, off)
	}

	// Fallback to memory read
	return n.readFromMemory(dest, off)
}

// readFromMemory reads data from the in-memory buffer
func (n *WSNode) readFromMemory(dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	if n.buf.Data == nil {
		return fuse.ReadResultData([]byte{}), 0
	}

	dataLen := int64(len(n.buf.Data))
	if off >= dataLen {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > dataLen {
		end = dataLen
	}

	result := n.buf.Data[off:end]
	return fuse.ReadResultData(result), 0
}

// readFromCacheFile reads data directly from the cache file (on-demand read)
func (n *WSNode) readFromCacheFile(dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	f, err := os.Open(n.buf.CachedPath)
	if err != nil {
		logging.Warnf("Failed to open cache file %s: %v", n.buf.CachedPath, err)
		// Cache file missing, clear and return error
		n.buf.CachedPath = ""
		n.buf.FileSize = 0
		return nil, syscall.EIO
	}
	defer f.Close()

	// Check bounds
	if off >= n.buf.FileSize {
		return fuse.ReadResultData([]byte{}), 0
	}

	end := off + int64(len(dest))
	if end > n.buf.FileSize {
		end = n.buf.FileSize
	}

	readSize := end - off
	buf := make([]byte, readSize)
	bytesRead, err := f.ReadAt(buf, off)
	if err != nil && err != io.EOF {
		logging.Warnf("Failed to read from cache file %s: %v", n.buf.CachedPath, err)
		return nil, syscall.EIO
	}

	return fuse.ReadResultData(buf[:bytesRead]), 0
}

func (n *WSNode) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Write called on path: %s, offset: %d, size: %d", n.fileInfo.Path, off, len(data))
	if off < 0 {
		return 0, syscall.EINVAL
	}

	// For writes, we need the data in memory
	if n.buf.Data == nil {
		// If cache path is set, load from cache file
		if n.buf.CachedPath != "" {
			cacheData, err := os.ReadFile(n.buf.CachedPath)
			if err != nil {
				logging.Warnf("Failed to load cache file for write %s: %v", n.buf.CachedPath, err)
				return 0, syscall.EIO
			}
			n.buf.Data = cacheData
			logging.Debugf("Loaded %d bytes from cache for write on %s", len(cacheData), n.fileInfo.Path)
		} else {
			// No cache path, call ensureDataLocked
			if errno := n.ensureDataLocked(ctx); errno != 0 {
				return 0, errno
			}
			// After ensureDataLocked, if CachedPath is set but Data is nil, load from cache
			if n.buf.Data == nil && n.buf.CachedPath != "" {
				cacheData, err := os.ReadFile(n.buf.CachedPath)
				if err != nil {
					logging.Warnf("Failed to load cache file for write %s: %v", n.buf.CachedPath, err)
					return 0, syscall.EIO
				}
				n.buf.Data = cacheData
				logging.Debugf("Loaded %d bytes from cache for write on %s", len(cacheData), n.fileInfo.Path)
			}
		}
	}

	// Ensure we have a buffer (may be empty for new files)
	if n.buf.Data == nil {
		n.buf.Data = []byte{}
	}

	end := off + int64(len(data))
	if int64(len(n.buf.Data)) < end {
		newData := make([]byte, end)
		copy(newData, n.buf.Data)
		n.buf.Data = newData
	}
	copy(n.buf.Data[off:], data)

	n.fileInfo.ObjectInfo.Size = int64(len(n.buf.Data))
	n.markModifiedLocked(time.Now())
	n.markDirtyLocked(dirtyData)

	return uint32(len(data)), 0
}

func (n *WSNode) Flush(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Flush called on path: %s", n.fileInfo.Path)
	if n.openCount > 0 {
		return 0
	}
	return n.flushLocked(ctx)
}

func (n *WSNode) Fsync(ctx context.Context, fh fs.FileHandle, flags uint32) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Fsync called on path: %s", n.fileInfo.Path)
	return n.flushLocked(ctx)
}

func (n *WSNode) Release(ctx context.Context, fh fs.FileHandle) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	logging.Debugf("Release called on path: %s", n.fileInfo.Path)

	n.decrementOpenLocked()
	if n.openCount > 0 {
		return 0
	}

	if !n.isDirtyLocked() {
		n.resetBufferLocked()
		return 0
	}

	errno := n.flushLocked(ctx)
	if errno == 0 {
		n.resetBufferLocked()
	}

	return errno
}
