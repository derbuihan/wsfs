package fuse

import (
	"context"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/buffer"
	"wsfs/internal/databricks"
)

func TestWSNodeTruncateLockedShrinks(t *testing.T) {
	n := &WSNode{
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Size:       10,
		}},
		buf: buffer.FileBuffer{Data: []byte("0123456789")},
	}

	n.truncateLocked(5)

	if got := string(n.buf.Data); got != "01234" {
		t.Fatalf("unexpected data after truncate: %q", got)
	}
	if got := n.fileInfo.Size(); got != 5 {
		t.Fatalf("unexpected size after truncate: %d", got)
	}
	if !n.buf.Dirty {
		t.Fatal("expected buffer to be dirty after truncate")
	}
}

func TestWSNodeWriteExtendsBuffer(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return []byte("hi"), nil
		},
	}
	n := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/test",
			Size:       2,
		}},
	}

	_, errno := n.Write(context.Background(), nil, []byte("!"), 5)
	if errno != 0 {
		t.Fatalf("unexpected errno: %d", errno)
	}
	if got := len(n.buf.Data); got != 6 {
		t.Fatalf("unexpected buffer length: %d", got)
	}
	if got := n.buf.Data[5]; got != '!' {
		t.Fatalf("unexpected last byte: %q", got)
	}
	if got := n.fileInfo.Size(); got != 6 {
		t.Fatalf("unexpected size after write: %d", got)
	}
	if !n.buf.Dirty {
		t.Fatal("expected buffer to be dirty after write")
	}
}
