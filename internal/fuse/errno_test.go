package fuse

import (
	"context"
	"fmt"
	iofs "io/fs"
	"syscall"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/databricks"
)

func testAPIError(statusCode int, errorCode string, message string) error {
	return &apierr.APIError{
		StatusCode: statusCode,
		ErrorCode:  errorCode,
		Message:    message,
	}
}

func TestErrnoFromBackendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   backendOp
		err  error
		want syscall.Errno
	}{
		{
			name: "not found",
			op:   backendOpLookup,
			err:  iofs.ErrNotExist,
			want: syscall.ENOENT,
		},
		{
			name: "permission denied",
			op:   backendOpRead,
			err:  testAPIError(403, "PERMISSION_DENIED", "forbidden"),
			want: syscall.EACCES,
		},
		{
			name: "already exists on create",
			op:   backendOpCreate,
			err:  testAPIError(409, "RESOURCE_ALREADY_EXISTS", "already exists"),
			want: syscall.EEXIST,
		},
		{
			name: "directory not empty api error",
			op:   backendOpDeleteDir,
			err:  testAPIError(400, "DIRECTORY_NOT_EMPTY", "Folder (/x) is not empty"),
			want: syscall.ENOTEMPTY,
		},
		{
			name: "directory not empty wrapped api error",
			op:   backendOpDeleteDir,
			err:  fmt.Errorf("delete failed: %w", testAPIError(400, "DIRECTORY_NOT_EMPTY", "Folder (/x) is not empty")),
			want: syscall.ENOTEMPTY,
		},
		{
			name: "directory not empty plain string error",
			op:   backendOpDeleteDir,
			err:  fmt.Errorf("DIRECTORY_NOT_EMPTY: Folder (/x) is not empty"),
			want: syscall.ENOTEMPTY,
		},
		{
			name: "invalid parameter",
			op:   backendOpRename,
			err:  testAPIError(400, "INVALID_PARAMETER_VALUE", "bad request"),
			want: syscall.EINVAL,
		},
		{
			name: "missing parent heuristic",
			op:   backendOpWrite,
			err:  testAPIError(400, "UNKNOWN", "RESOURCE_DOES_NOT_EXIST: The parent folder (/tmp) does not exist."),
			want: syscall.ENOENT,
		},
		{
			name: "fallback to eio",
			op:   backendOpWrite,
			err:  testAPIError(500, "UNKNOWN", "backend exploded"),
			want: syscall.EIO,
		},
		{
			name: "delete dir unrelated unknown stays eio",
			op:   backendOpDeleteDir,
			err:  testAPIError(500, "UNKNOWN", "backend exploded"),
			want: syscall.EIO,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := errnoFromBackendError(tt.op, tt.err); got != tt.want {
				t.Fatalf("expected errno %d, got %d", tt.want, got)
			}
		})
	}
}

func TestWSNodeEnsureDataLockedMapsPermissionDenied(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		ReadAllFunc: func(ctx context.Context, filePath string) ([]byte, error) {
			return nil, testAPIError(403, "PERMISSION_DENIED", "forbidden")
		},
	}

	node := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/forbidden.txt",
		}},
	}

	if errno := node.ensureDataLocked(context.Background()); errno != syscall.EACCES {
		t.Fatalf("expected EACCES, got %d", errno)
	}
}

func TestWSNodeFlushLockedMapsMissingParentToENOENT(t *testing.T) {
	api := &databricks.FakeWorkspaceAPI{
		WriteFunc: func(ctx context.Context, filepath string, data []byte) error {
			return testAPIError(400, "UNKNOWN", "RESOURCE_DOES_NOT_EXIST: The parent folder (/gone) does not exist.")
		},
	}

	node := &WSNode{
		wfClient: api,
		fileInfo: databricks.WSFileInfo{ObjectInfo: workspace.ObjectInfo{
			ObjectType: workspace.ObjectTypeFile,
			Path:       "/gone/file.txt",
		}},
		buf: fileBuffer{Data: []byte("data")},
	}
	node.markDirtyLocked(dirtyData)

	if errno := node.flushLocked(context.Background()); errno != syscall.ENOENT {
		t.Fatalf("expected ENOENT, got %d", errno)
	}
	if !node.isDirtyLocked() {
		t.Fatal("expected node to remain dirty after failed flush")
	}
}
