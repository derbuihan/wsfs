package fuse

import (
	"context"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	"wsfs/internal/logging"
)

func (n *WSNode) rememberNotebookExactSizeLocked(size int64) {
	if !n.fileInfo.IsNotebook() {
		return
	}
	if n.fileInfo.NotebookSizeComputed && n.fileInfo.Size() == size {
		return
	}
	n.fileInfo.ObjectInfo.Size = size
	n.fileInfo.NotebookSizeComputed = true
	if n.wfClient != nil {
		n.wfClient.CacheSet(n.Path(), n.fileInfo)
	}
}

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
		cachedPath, checksum, found := n.diskCache.Get(remotePath, remoteModTime)
		if found {
			// Verify cache file exists
			if info, err := os.Stat(cachedPath); err == nil {
				n.buf.CachedPath = cachedPath
				n.buf.CachedChecksum = checksum
				n.buf.FileSize = info.Size()
				n.rememberNotebookExactSizeLocked(info.Size())
				logging.Debugf("Cache path set for %s (on-demand read)", remotePath)
				return 0
			}
			// Cache file missing, delete entry and fall through to remote read
			logging.Debugf("Cache file missing for %s, fetching from remote", remotePath)
			n.deleteDiskCacheEntries(remotePath)
		}
	}

	// Cache miss or disabled - read from remote
	logging.Debugf("Cache miss for %s, fetching from remote", remotePath)
	readCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()
	data, err := n.wfClient.ReadAll(readCtx, remotePath)
	if err != nil {
		logging.Debugf("Failed to read file %s: %v", remotePath, err)
		return errnoFromBackendError(backendOpRead, err)
	}

	// Store in cache and use cache path for on-demand reads
	if n.diskCache != nil && !n.diskCache.IsDisabled() {
		localPath, err := n.diskCache.Set(remotePath, data, remoteModTime)
		if err == nil {
			n.buf.CachedPath = localPath
			n.buf.CachedChecksum = filecache.CalculateChecksum(data)
			n.buf.FileSize = int64(len(data))
			n.rememberNotebookExactSizeLocked(int64(len(data)))
			logging.Debugf("Cached file %s (%d bytes), using on-demand read", remotePath, len(data))
			return 0
		}
		// Cache set failed, fall back to memory
		logging.Debugf("Failed to cache file %s: %v, using memory", remotePath, err)
	}

	// Fallback: keep data in memory (when cache is disabled or failed)
	n.buf.Data = data
	n.buf.FileSize = int64(len(data))
	n.rememberNotebookExactSizeLocked(int64(len(data)))
	return 0
}

func (n *WSNode) invalidateCurrentCacheLocked() {
	currentPath := n.Path()
	n.clearCachedFileLocked()
	n.deleteDiskCacheEntries(currentPath)
}

func (n *WSNode) loadDataFromCacheLocked(ctx context.Context) syscall.Errno {
	readCache := func() ([]byte, error) {
		data, err := os.ReadFile(n.buf.CachedPath)
		if err != nil {
			return nil, err
		}
		if n.buf.CachedChecksum != "" {
			actualChecksum := filecache.CalculateChecksum(data)
			if actualChecksum != n.buf.CachedChecksum {
				return nil, fmt.Errorf("cache checksum mismatch for %s (expected %s, got %s)", n.Path(), truncateChecksum(n.buf.CachedChecksum), truncateChecksum(actualChecksum))
			}
		}
		return data, nil
	}

	if n.buf.CachedPath == "" {
		return 0
	}

	data, err := readCache()
	if err == nil {
		n.buf.Data = data
		n.buf.FileSize = int64(len(data))
		n.rememberNotebookExactSizeLocked(int64(len(data)))
		return 0
	}

	logging.Warnf("Failed to load cached data for mutation %s: %v", n.Path(), err)
	n.invalidateCurrentCacheLocked()
	if errno := n.ensureDataLocked(ctx); errno != 0 {
		return errno
	}
	if n.buf.Data != nil {
		return 0
	}
	if n.buf.CachedPath == "" {
		return 0
	}

	data, err = readCache()
	if err != nil {
		logging.Warnf("Failed to reload cached data for mutation %s after remote fetch: %v", n.Path(), err)
		n.invalidateCurrentCacheLocked()
		return syscall.EIO
	}
	n.buf.Data = data
	n.buf.FileSize = int64(len(data))
	n.rememberNotebookExactSizeLocked(int64(len(data)))
	return 0
}

func (n *WSNode) ensureDataForMutationLocked(ctx context.Context) syscall.Errno {
	if n.buf.Data != nil {
		return 0
	}
	if n.buf.CachedPath != "" {
		if errno := n.loadDataFromCacheLocked(ctx); errno != 0 {
			return errno
		}
		return 0
	}
	if errno := n.ensureDataLocked(ctx); errno != 0 {
		return errno
	}
	if n.buf.Data == nil && n.buf.CachedPath != "" {
		if errno := n.loadDataFromCacheLocked(ctx); errno != 0 {
			return errno
		}
	}
	return 0
}

func (n *WSNode) truncateLocked(size uint64) {
	if size == 0 {
		n.buf.Data = []byte{}
	} else {
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

func (n *WSNode) applyBufferedMetadataFallbackLocked(now time.Time) {
	if n.buf.Data != nil {
		n.fileInfo.ObjectInfo.Size = int64(len(n.buf.Data))
	}
	n.markModifiedLocked(now)
	n.metadataCheckedAt = now
}

func (n *WSNode) flushLocked(ctx context.Context) syscall.Errno {
	if !n.isDirtyLocked() || n.buf.Data == nil {
		return 0
	}

	// Apply timeout for write and metadata refresh operations.
	opCtx, cancel := context.WithTimeout(ctx, dataOpTimeout)
	defer cancel()

	remotePath := n.Path()
	err := n.wfClient.Write(opCtx, remotePath, n.buf.Data)
	if err != nil {
		logging.Warnf("Error writing back on Flush for %s: %v", remotePath, err)
		return errnoFromBackendError(backendOpWrite, err)
	}
	n.clearDirtyLocked()

	if info, err := n.wfClient.StatFresh(opCtx, remotePath); err != nil {
		logging.Warnf("Error refreshing file info after Flush for %s: %v", remotePath, err)
		n.applyBufferedMetadataFallbackLocked(time.Now())
	} else if wsInfo, ok := info.(databricks.WSFileInfo); !ok {
		logging.Warnf("Unexpected file info type after Flush for %s", remotePath)
		n.applyBufferedMetadataFallbackLocked(time.Now())
	} else {
		n.fileInfo = wsInfo
		n.metadataCheckedAt = time.Now()
	}

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

	metadataChanged := false
	if changed, errno := n.refreshMetadataLocked(ctx, false); errno != 0 {
		return nil, 0, errno
	} else {
		metadataChanged = changed
	}

	if flags&syscall.O_TRUNC != 0 {
		// Invalidate caches immediately for truncation
		n.wfClient.CacheInvalidate(n.Path())
		n.deleteDiskCacheEntries(n.fileInfo.Path)

		n.clearCachedFileLocked()
		n.truncateLocked(0)
		n.markModifiedLocked(time.Now())
		n.metadataCheckedAt = time.Now()
	}

	openFlags := uint32(0)
	if flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_TRUNC) != 0 {
		openFlags |= fuse.FOPEN_DIRECT_IO
	} else if metadataChanged {
		openFlags |= fuse.FOPEN_DIRECT_IO
	} else if n.fileInfo.IsNotebook() && !n.fileInfo.NotebookSizeComputed {
		openFlags |= fuse.FOPEN_DIRECT_IO
	} else {
		openFlags |= fuse.FOPEN_KEEP_CACHE
	}

	n.incrementOpenLocked()

	return &wsFileHandle{}, openFlags, 0
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
		result, errno := n.readFromCacheFile(dest, off)
		if errno == 0 {
			return result, 0
		}
		if n.buf.CachedPath == "" && n.buf.Data == nil {
			if errno := n.ensureDataLocked(ctx); errno != 0 {
				return nil, errno
			}
			if n.buf.CachedPath != "" {
				return n.readFromCacheFile(dest, off)
			}
			return n.readFromMemory(dest, off)
		}
		return nil, errno
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
		result, errno := n.readFromCacheFile(dest, off)
		if errno == 0 {
			return result, 0
		}
		if n.buf.CachedPath == "" && n.buf.Data == nil {
			if errno := n.ensureDataLocked(ctx); errno != 0 {
				return nil, errno
			}
			if n.buf.CachedPath != "" {
				return n.readFromCacheFile(dest, off)
			}
			return n.readFromMemory(dest, off)
		}
		return nil, errno
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
		n.invalidateCurrentCacheLocked()
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
		n.invalidateCurrentCacheLocked()
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
		if errno := n.ensureDataForMutationLocked(ctx); errno != 0 {
			return 0, errno
		}
	}

	// Ensure we have a buffer (may be empty for new files)
	if n.buf.Data == nil {
		n.buf.Data = []byte{}
	}
	if n.buf.ReplaceOnFirstWrite && off == 0 {
		n.buf.Data = []byte{}
		n.clearCachedFileLocked()
	}

	end := off + int64(len(data))
	if int64(len(n.buf.Data)) < end {
		newData := make([]byte, end)
		copy(newData, n.buf.Data)
		n.buf.Data = newData
	}
	copy(n.buf.Data[off:], data)
	n.buf.ReplaceOnFirstWrite = false

	n.fileInfo.ObjectInfo.Size = int64(len(n.buf.Data))
	n.markModifiedLocked(time.Now())
	n.metadataCheckedAt = time.Now()
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
