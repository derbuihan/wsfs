package fuse

import (
	"context"
	"fmt"
	"sync"

	"wsfs/internal/logging"
)

// DirtyNodeRegistry tracks WSNode instances with dirty buffers.
// It is used during graceful shutdown to flush all dirty buffers
// before unmounting the filesystem.
type DirtyNodeRegistry struct {
	nodes map[*WSNode]struct{}
	mu    sync.RWMutex
}

// NewDirtyNodeRegistry creates a new registry.
func NewDirtyNodeRegistry() *DirtyNodeRegistry {
	return &DirtyNodeRegistry{
		nodes: make(map[*WSNode]struct{}),
	}
}

// Register adds a node to the registry.
// This should be called when a node's buffer becomes dirty.
func (r *DirtyNodeRegistry) Register(node *WSNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node] = struct{}{}
}

// Unregister removes a node from the registry.
// This should be called after a node's buffer has been flushed.
func (r *DirtyNodeRegistry) Unregister(node *WSNode) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, node)
}

// FlushAll flushes all dirty nodes.
// Returns the number of nodes flushed and any errors encountered.
func (r *DirtyNodeRegistry) FlushAll(ctx context.Context) (int, []error) {
	r.mu.RLock()
	// Copy nodes to avoid holding lock during flush
	nodes := make([]*WSNode, 0, len(r.nodes))
	for node := range r.nodes {
		nodes = append(nodes, node)
	}
	r.mu.RUnlock()

	var errors []error
	flushed := 0

	for _, node := range nodes {
		// Check context cancellation
		select {
		case <-ctx.Done():
			errors = append(errors, fmt.Errorf("context cancelled during flush"))
			return flushed, errors
		default:
		}

		logging.Debugf("Flushing dirty buffer for: %s", node.Path())

		node.mu.Lock()
		if node.buf.Dirty {
			errno := node.flushLocked(ctx)
			if errno != 0 {
				errors = append(errors, fmt.Errorf("flush %s: errno %d", node.Path(), errno))
			} else {
				flushed++
			}
		}
		node.mu.Unlock()
	}

	return flushed, errors
}

// Count returns the number of dirty nodes.
func (r *DirtyNodeRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}
