package databricks

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"
)

// FakeWorkspaceAPI is a test double for WorkspaceFilesAPI
type FakeWorkspaceAPI struct {
	StatFunc    func(ctx context.Context, filePath string) (fs.FileInfo, error)
	ReadDirFunc func(ctx context.Context, dirPath string) ([]fs.DirEntry, error)
	ReadAllFunc func(ctx context.Context, filePath string) ([]byte, error)
	WriteFunc   func(ctx context.Context, filepath string, data []byte) error
	DeleteFunc  func(ctx context.Context, filePath string, recursive bool) error
	MkdirFunc   func(ctx context.Context, dirPath string) error
	RenameFunc  func(ctx context.Context, sourcePath string, destinationPath string) error
	CacheSetFunc func(path string, info fs.FileInfo)
}

func (f *FakeWorkspaceAPI) Stat(ctx context.Context, filePath string) (fs.FileInfo, error) {
	if f.StatFunc != nil {
		return f.StatFunc(ctx, filePath)
	}
	return nil, fs.ErrNotExist
}

func (f *FakeWorkspaceAPI) ReadDir(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
	if f.ReadDirFunc != nil {
		return f.ReadDirFunc(ctx, dirPath)
	}
	return nil, fs.ErrNotExist
}

func (f *FakeWorkspaceAPI) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	if f.ReadAllFunc != nil {
		return f.ReadAllFunc(ctx, filePath)
	}
	return nil, fs.ErrNotExist
}

func (f *FakeWorkspaceAPI) Write(ctx context.Context, filepath string, data []byte) error {
	if f.WriteFunc != nil {
		return f.WriteFunc(ctx, filepath, data)
	}
	return nil
}

func (f *FakeWorkspaceAPI) Delete(ctx context.Context, filePath string, recursive bool) error {
	if f.DeleteFunc != nil {
		return f.DeleteFunc(ctx, filePath, recursive)
	}
	return nil
}

func (f *FakeWorkspaceAPI) Mkdir(ctx context.Context, dirPath string) error {
	if f.MkdirFunc != nil {
		return f.MkdirFunc(ctx, dirPath)
	}
	return nil
}

func (f *FakeWorkspaceAPI) Rename(ctx context.Context, sourcePath string, destinationPath string) error {
	if f.RenameFunc != nil {
		return f.RenameFunc(ctx, sourcePath, destinationPath)
	}
	return nil
}

func (f *FakeWorkspaceAPI) CacheSet(path string, info fs.FileInfo) {
	if f.CacheSetFunc != nil {
		f.CacheSetFunc(path, info)
	}
}

// MockWorkspaceClient is a mock for the workspaceClient interface (thin wrapper)
// This only implements the methods we actually use: Export, Delete, Mkdirs, Import
type MockWorkspaceClient struct {
	ExportFunc func(ctx context.Context, request workspace.ExportRequest) (*workspace.ExportResponse, error)
	DeleteFunc func(ctx context.Context, request workspace.Delete) error
	MkdirsFunc func(ctx context.Context, request workspace.Mkdirs) error
	ImportFunc func(ctx context.Context, request workspace.Import) error
}

func (m *MockWorkspaceClient) Export(ctx context.Context, request workspace.ExportRequest) (*workspace.ExportResponse, error) {
	if m.ExportFunc != nil {
		return m.ExportFunc(ctx, request)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *MockWorkspaceClient) Delete(ctx context.Context, request workspace.Delete) error {
	if m.DeleteFunc != nil {
		return m.DeleteFunc(ctx, request)
	}
	return fmt.Errorf("not implemented")
}

func (m *MockWorkspaceClient) Mkdirs(ctx context.Context, request workspace.Mkdirs) error {
	if m.MkdirsFunc != nil {
		return m.MkdirsFunc(ctx, request)
	}
	return fmt.Errorf("not implemented")
}

func (m *MockWorkspaceClient) Import(ctx context.Context, request workspace.Import) error {
	if m.ImportFunc != nil {
		return m.ImportFunc(ctx, request)
	}
	return fmt.Errorf("not implemented")
}

// MockAPIClient is a mock for apiDoer interface
type MockAPIClient struct {
	DoFunc func(ctx context.Context, method, path string,
		headers map[string]string, queryParams map[string]any, request, response any,
		visitors ...func(*http.Request) error) error
}

func (m *MockAPIClient) Do(ctx context.Context, method, path string,
	headers map[string]string, queryParams map[string]any, request, response any,
	visitors ...func(*http.Request) error) error {
	if m.DoFunc != nil {
		return m.DoFunc(ctx, method, path, headers, queryParams, request, response, visitors...)
	}
	return fmt.Errorf("not implemented")
}

// Helper functions for creating test data

func NewTestFileInfo(path string, size int64, isDir bool) WSFileInfo {
	objType := workspace.ObjectTypeFile
	if isDir {
		objType = workspace.ObjectTypeDirectory
	}

	return WSFileInfo{
		ObjectInfo: workspace.ObjectInfo{
			Path:       path,
			ObjectType: objType,
			Size:       size,
			ModifiedAt: time.Now().UnixMilli(),
		},
	}
}

func NewTestFileInfoWithSignedURL(path string, size int64, url string, headers map[string]string) WSFileInfo {
	info := NewTestFileInfo(path, size, false)
	info.SignedURL = url
	info.SignedURLHeaders = headers
	return info
}

// InMemoryFileSystem is a simple in-memory filesystem for testing
type InMemoryFileSystem struct {
	files map[string][]byte
	dirs  map[string]bool
}

func NewInMemoryFileSystem() *InMemoryFileSystem {
	return &InMemoryFileSystem{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (fs *InMemoryFileSystem) SetFile(path string, content []byte) {
	fs.files[path] = content
}

func (fs *InMemoryFileSystem) GetFile(path string) ([]byte, bool) {
	content, exists := fs.files[path]
	return content, exists
}

func (fs *InMemoryFileSystem) SetDir(path string) {
	fs.dirs[path] = true
}

func (fs *InMemoryFileSystem) IsDir(path string) bool {
	return fs.dirs[path]
}

func (fs *InMemoryFileSystem) ListFiles(dirPath string) []string {
	var result []string
	prefix := strings.TrimSuffix(dirPath, "/") + "/"

	for filePath := range fs.files {
		if strings.HasPrefix(filePath, prefix) {
			// Only include direct children, not nested files
			remaining := strings.TrimPrefix(filePath, prefix)
			if !strings.Contains(remaining, "/") {
				result = append(result, filePath)
			}
		}
	}

	for dirPath := range fs.dirs {
		if strings.HasPrefix(dirPath, prefix) && dirPath != prefix {
			remaining := strings.TrimPrefix(dirPath, prefix)
			if !strings.Contains(strings.TrimSuffix(remaining, "/"), "/") {
				result = append(result, dirPath)
			}
		}
	}

	return result
}

func (fs *InMemoryFileSystem) Delete(path string) {
	delete(fs.files, path)
	delete(fs.dirs, path)
}
