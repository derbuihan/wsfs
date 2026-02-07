package fuse

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	"wsfs/internal/logging"
)

// File system constants
const (
	// Attribute and entry cache timeouts in seconds
	attrTimeoutSec  = 60
	entryTimeoutSec = 60

	// File permissions
	dirMode  = 0755
	fileMode = 0644

	// Block size for file attributes
	blockSize   = 4096
	blockFactor = 512 // for calculating number of blocks

	// Statfs limits
	maxNameLen = 255

	// Default inode number when no ID is available
	defaultIno = 1

	// Nlink values
	dirNlink  = 2
	fileNlink = 1
)

// Operation timeouts for API calls
const (
	// dataOpTimeout is used for read/write operations that may involve large files
	dataOpTimeout = 2 * time.Minute

	// metadataOpTimeout is used for stat, delete, mkdir, rename operations
	metadataOpTimeout = 30 * time.Second

	// dirListTimeout is used for directory listing operations
	dirListTimeout = 1 * time.Minute
)

// fileBuffer holds in-memory file data and dirty state.
// For memory efficiency, CachedPath can be set instead of Data to read directly from cache.
type fileBuffer struct {
	Data       []byte
	Dirty      bool
	CachedPath string // Path to cached file for on-demand reading
	FileSize   int64  // File size for cached file reads
}

// NodeConfig holds configuration for access control.
type NodeConfig struct {
	OwnerUid       uint32 // UID of the user who mounted the filesystem
	RestrictAccess bool   // Whether to enforce UID-based access control
}

type dirtyFlag uint8

const (
	dirtyData dirtyFlag = 1 << iota
	dirtyTruncate
)

type WSNode struct {
	fs.Inode
	wfClient        databricks.WorkspaceFilesAPI
	diskCache       *filecache.DiskCache
	fileInfo        databricks.WSFileInfo
	buf             fileBuffer
	mu              sync.Mutex
	registry        *DirtyNodeRegistry
	ownerUid        uint32 // UID of the mount owner
	restrictAccess  bool   // Enforce access control when true
	openCount       int
	dirtyFlags      dirtyFlag
	pendingTruncate bool
}

var _ = (fs.NodeGetattrer)((*WSNode)(nil))
var _ = (fs.NodeSetattrer)((*WSNode)(nil))
var _ = (fs.NodeReaddirer)((*WSNode)(nil))
var _ = (fs.NodeLookuper)((*WSNode)(nil))
var _ = (fs.NodeOpener)((*WSNode)(nil))
var _ = (fs.NodeOpendirer)((*WSNode)(nil))
var _ = (fs.NodeOpendirHandler)((*WSNode)(nil))
var _ = (fs.NodeReader)((*WSNode)(nil))
var _ = (fs.NodeWriter)((*WSNode)(nil))
var _ = (fs.NodeFlusher)((*WSNode)(nil))
var _ = (fs.NodeFsyncer)((*WSNode)(nil))
var _ = (fs.NodeReleaser)((*WSNode)(nil))
var _ = (fs.NodeCreater)((*WSNode)(nil))
var _ = (fs.NodeUnlinker)((*WSNode)(nil))
var _ = (fs.NodeMkdirer)((*WSNode)(nil))
var _ = (fs.NodeRmdirer)((*WSNode)(nil))
var _ = (fs.NodeRenamer)((*WSNode)(nil))
var _ = (fs.NodeAccesser)((*WSNode)(nil))
var _ = (fs.NodeStatfser)((*WSNode)(nil))
var _ = (fs.NodeOnForgetter)((*WSNode)(nil))

func (n *WSNode) Path() string {
	return n.fileInfo.Path
}

func stableIno(info databricks.WSFileInfo) uint64 {
	if info.ObjectId > 0 {
		return uint64(info.ObjectId)
	}
	if info.ResourceId != "" {
		return hashStringToIno(info.ResourceId)
	}
	if info.Path != "" {
		return hashStringToIno(info.Path)
	}
	return defaultIno
}

func hashStringToIno(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	sum := h.Sum64()
	if sum == 0 {
		return defaultIno
	}
	return sum
}

// truncateChecksum returns the first 8 characters of a checksum for logging
func truncateChecksum(checksum string) string {
	if len(checksum) > 8 {
		return checksum[:8]
	}
	return checksum
}

func (n *WSNode) markDirtyLocked(flag dirtyFlag) {
	n.dirtyFlags |= flag
	n.buf.Dirty = true
	if n.registry != nil {
		n.registry.Register(n)
	}
}

func (n *WSNode) clearDirtyLocked() {
	n.dirtyFlags = 0
	n.buf.Dirty = false
	n.pendingTruncate = false
	if n.registry != nil {
		n.registry.Unregister(n)
	}
}

func (n *WSNode) isDirtyLocked() bool {
	return n.buf.Dirty || n.dirtyFlags != 0
}

func (n *WSNode) shouldFlushNowLocked() bool {
	return n.isDirtyLocked() && n.openCount == 0
}

func (n *WSNode) incrementOpenLocked() {
	n.openCount++
}

func (n *WSNode) decrementOpenLocked() {
	if n.openCount > 0 {
		n.openCount--
		return
	}
	logging.Warnf("Release called with openCount=0 for %s", n.Path())
}

func (n *WSNode) markModifiedLocked(t time.Time) {
	n.fileInfo.ObjectInfo.ModifiedAt = t.UnixMilli()
}

func (n *WSNode) resetBufferLocked() {
	n.buf.Data = nil
	n.buf.CachedPath = ""
	n.buf.FileSize = 0
	n.clearDirtyLocked()
}

func NewRootNode(wfClient databricks.WorkspaceFilesAPI, diskCache *filecache.DiskCache, rootPath string, registry *DirtyNodeRegistry, config *NodeConfig) (*WSNode, error) {
	info, err := wfClient.Stat(context.Background(), rootPath)

	if err != nil {
		return nil, err
	}

	wsInfo, ok := info.(databricks.WSFileInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected file info type for root path %s", rootPath)
	}
	if !wsInfo.IsDir() {
		return nil, syscall.ENOTDIR
	}

	node := &WSNode{
		wfClient:  wfClient,
		diskCache: diskCache,
		fileInfo:  wsInfo,
		registry:  registry,
	}

	// Apply access control configuration
	if config != nil {
		node.ownerUid = config.OwnerUid
		node.restrictAccess = config.RestrictAccess
	}

	return node, nil
}
