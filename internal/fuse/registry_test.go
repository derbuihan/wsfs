package fuse

import (
	"context"
	"testing"
)

func TestDirtyNodeRegistry_RegisterUnregister(t *testing.T) {
	registry := NewDirtyNodeRegistry()

	node := &WSNode{}

	// Initially empty
	if registry.Count() != 0 {
		t.Errorf("Expected 0 nodes, got %d", registry.Count())
	}

	// Register
	registry.Register(node)
	if registry.Count() != 1 {
		t.Errorf("Expected 1 node, got %d", registry.Count())
	}

	// Duplicate register (should not increase count)
	registry.Register(node)
	if registry.Count() != 1 {
		t.Errorf("Expected 1 node after duplicate, got %d", registry.Count())
	}

	// Unregister
	registry.Unregister(node)
	if registry.Count() != 0 {
		t.Errorf("Expected 0 nodes after unregister, got %d", registry.Count())
	}
}

func TestDirtyNodeRegistry_MultipleNodes(t *testing.T) {
	registry := NewDirtyNodeRegistry()

	node1 := &WSNode{}
	node2 := &WSNode{}
	node3 := &WSNode{}

	// Register multiple nodes
	registry.Register(node1)
	registry.Register(node2)
	registry.Register(node3)

	if registry.Count() != 3 {
		t.Errorf("Expected 3 nodes, got %d", registry.Count())
	}

	// Unregister one
	registry.Unregister(node2)
	if registry.Count() != 2 {
		t.Errorf("Expected 2 nodes, got %d", registry.Count())
	}

	// Unregister non-existent node (should not panic)
	registry.Unregister(&WSNode{})
	if registry.Count() != 2 {
		t.Errorf("Expected 2 nodes after unregistering non-existent, got %d", registry.Count())
	}

	// Unregister remaining
	registry.Unregister(node1)
	registry.Unregister(node3)
	if registry.Count() != 0 {
		t.Errorf("Expected 0 nodes, got %d", registry.Count())
	}
}

func TestDirtyNodeRegistry_FlushAll_Empty(t *testing.T) {
	registry := NewDirtyNodeRegistry()

	flushed, errors := registry.FlushAll(context.Background())

	if flushed != 0 {
		t.Errorf("Expected 0 flushed, got %d", flushed)
	}
	if len(errors) != 0 {
		t.Errorf("Expected 0 errors, got %d", len(errors))
	}
}

func TestDirtyNodeRegistry_FlushAll_CancelledContext(t *testing.T) {
	registry := NewDirtyNodeRegistry()

	// Register a node
	node := &WSNode{}
	registry.Register(node)

	// Create cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	flushed, errors := registry.FlushAll(ctx)

	// Should return early due to cancelled context
	if flushed != 0 {
		t.Errorf("Expected 0 flushed, got %d", flushed)
	}
	if len(errors) != 1 {
		t.Errorf("Expected 1 error (context cancelled), got %d", len(errors))
	}
}
