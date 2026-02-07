package fuse

import (
	"context"
	"io/fs"
	"syscall"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/databricks"
)

type dummyFileInfo struct{}

func (d dummyFileInfo) Name() string       { return "dummy" }
func (d dummyFileInfo) Size() int64        { return 0 }
func (d dummyFileInfo) Mode() fs.FileMode  { return 0 }
func (d dummyFileInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (d dummyFileInfo) IsDir() bool        { return false }
func (d dummyFileInfo) Sys() any           { return nil }

func TestStableInoVariants(t *testing.T) {
	infoWithObject := databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectId: 42}}
	if got := stableIno(infoWithObject); got != 42 {
		t.Fatalf("expected ObjectId to win, got %d", got)
	}

	infoWithResource := databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{ResourceId: "res-123"}}
	expected := hashStringToIno("res-123")
	if got := stableIno(infoWithResource); got != expected {
		t.Fatalf("expected resource hash %d, got %d", expected, got)
	}

	infoWithPath := databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{Path: "/path"}}
	expected = hashStringToIno("/path")
	if got := stableIno(infoWithPath); got != expected {
		t.Fatalf("expected path hash %d, got %d", expected, got)
	}

	if got := stableIno(databricks.WSFileInfo{}); got != defaultIno {
		t.Fatalf("expected default ino %d, got %d", defaultIno, got)
	}
}

func TestTruncateChecksum(t *testing.T) {
	if got := truncateChecksum("12345678"); got != "12345678" {
		t.Fatalf("unexpected checksum: %s", got)
	}
	if got := truncateChecksum("1234567890"); got != "12345678" {
		t.Fatalf("unexpected truncated checksum: %s", got)
	}
}

func TestNewRootNode(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, true), nil
		},
	}
	config := &NodeConfig{OwnerUid: 99, RestrictAccess: true}
	root, err := NewRootNode(api, nil, "/", NewDirtyNodeRegistry(), config)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if root.ownerUid != 99 || !root.restrictAccess {
		t.Fatalf("unexpected node config: %+v", root)
	}
}

func TestNewRootNode_NotDir(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return databricks.NewTestFileInfo(filePath, 0, false), nil
		},
	}
	_, err := NewRootNode(api, nil, "/", NewDirtyNodeRegistry(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if err != syscall.ENOTDIR {
		t.Fatalf("expected ENOTDIR, got %v", err)
	}
}

func TestNewRootNode_UnexpectedType(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		StatFunc: func(ctx context.Context, filePath string) (fs.FileInfo, error) {
			return dummyFileInfo{}, nil
		},
	}
	_, err := NewRootNode(api, nil, "/", NewDirtyNodeRegistry(), nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
