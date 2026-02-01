package databricks

import (
	"context"
	"io/fs"
)

// WorkspaceFilesAPI defines the minimal surface WSNode needs.
// It allows swapping in test doubles without touching node logic.
type WorkspaceFilesAPI interface {
	Stat(ctx context.Context, filePath string) (fs.FileInfo, error)
	ReadDir(ctx context.Context, dirPath string) ([]fs.DirEntry, error)
	ReadAll(ctx context.Context, filePath string) ([]byte, error)
	Write(ctx context.Context, filepath string, data []byte) error
	Delete(ctx context.Context, filePath string, recursive bool) error
	Mkdir(ctx context.Context, dirPath string) error
	Rename(ctx context.Context, sourcePath string, destinationPath string) error
	CacheSet(path string, info fs.FileInfo)
}
