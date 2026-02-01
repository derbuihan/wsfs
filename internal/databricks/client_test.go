package databricks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/cache"
)

// TestStatCaching verifies that Stat caches results correctly
func TestStatCaching(t *testing.T) {
	callCount := 0
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			callCount++
			if strings.Contains(path, "object-info") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       100,
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	// First call should hit the API
	info1, err := client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if callCount != 1 {
		t.Errorf("Expected 1 API call, got %d", callCount)
	}

	// Second call should use cache
	info2, err := client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if callCount != 1 {
		t.Errorf("Expected 1 API call (cached), got %d", callCount)
	}

	// Results should be the same
	if info1.Name() != info2.Name() || info1.Size() != info2.Size() {
		t.Errorf("Cached result differs from original")
	}
}

// TestStatNotFound verifies that Stat caches not-found results
func TestStatNotFound(t *testing.T) {
	callCount := 0
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			callCount++
			return fs.ErrNotExist
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	// First call should hit the API
	_, err1 := client.Stat(context.Background(), "/nonexistent.txt")
	if err1 == nil {
		t.Fatal("Expected error for non-existent file")
	}
	if callCount != 1 {
		t.Errorf("Expected 1 API call, got %d", callCount)
	}

	// Second call should use cached error
	_, err2 := client.Stat(context.Background(), "/nonexistent.txt")
	if err2 == nil {
		t.Fatal("Expected error for non-existent file")
	}
	if callCount != 1 {
		t.Errorf("Expected 1 API call (cached error), got %d", callCount)
	}
}

// TestReadAllViaSignedURL verifies that ReadAll uses signed URL when available
func TestReadAllViaSignedURL(t *testing.T) {
	testContent := []byte("test content via signed URL")

	// Set up a test HTTP server to simulate signed URL endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("Expected GET request, got %s", r.Method)
		}
		if r.Header.Get("X-Test-Header") != "test-value" {
			t.Errorf("Expected custom header, got %s", r.Header.Get("X-Test-Header"))
		}
		w.WriteHeader(http.StatusOK)
		w.Write(testContent)
	}))
	defer server.Close()

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       int64(len(testContent)),
						ModifiedAt: time.Now().UnixMilli(),
					},
					SignedURL: &struct {
						URL     string            `json:"url"`
						Headers map[string]string `json:"headers,omitempty"`
					}{
						URL:     server.URL,
						Headers: map[string]string{"X-Test-Header": "test-value"},
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	data, err := client.ReadAll(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != string(testContent) {
		t.Errorf("Expected content %q, got %q", string(testContent), string(data))
	}
}

// TestReadAllFallbackToExport verifies that ReadAll falls back to Export when signed URL fails
func TestReadAllFallbackToExport(t *testing.T) {
	testContent := []byte("test content via Export")
	contentB64 := base64.StdEncoding.EncodeToString(testContent)

	signedURLCalled := false
	exportCalled := false

	// Set up a test HTTP server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signedURLCalled = true
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       int64(len(testContent)),
						ModifiedAt: time.Now().UnixMilli(),
					},
					SignedURL: &struct {
						URL     string            `json:"url"`
						Headers map[string]string `json:"headers,omitempty"`
					}{
						URL:     server.URL,
						Headers: map[string]string{},
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	mockWorkspace := &MockWorkspaceClient{
		ExportFunc: func(ctx context.Context, request workspace.ExportRequest) (*workspace.ExportResponse, error) {
			exportCalled = true
			return &workspace.ExportResponse{
				Content: contentB64,
			}, nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, mockAPI, nil)

	data, err := client.ReadAll(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !signedURLCalled {
		t.Error("Expected signed URL to be called")
	}

	if !exportCalled {
		t.Error("Expected Export fallback to be called")
	}

	if string(data) != string(testContent) {
		t.Errorf("Expected content %q, got %q", string(testContent), string(data))
	}
}

// TestWriteViaNewFiles verifies that Write uses new-files API
func TestWriteViaNewFiles(t *testing.T) {
	testContent := []byte("test content for new-files")
	signedURLCalled := false

	// Set up a test HTTP server to simulate signed URL PUT
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("Expected PUT request, got %s", r.Method)
		}
		signedURLCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "new-files") {
				resp := response.(*struct {
					SignedURLs []struct {
						URL     string            `json:"url"`
						Headers map[string]string `json:"headers"`
					} `json:"signed_urls"`
				})
				resp.SignedURLs = []struct {
					URL     string            `json:"url"`
					Headers map[string]string `json:"headers"`
				}{
					{
						URL:     server.URL,
						Headers: map[string]string{},
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, cache.NewCache(1*time.Second))

	err := client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !signedURLCalled {
		t.Error("Expected signed URL PUT to be called")
	}
}

// TestWriteFallbackToImportFile verifies that Write falls back to import-file
func TestWriteFallbackToImportFile(t *testing.T) {
	testContent := []byte("test content for import-file")
	importFileCalled := false

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "new-files") {
				return fmt.Errorf("new-files API error")
			}
			if strings.Contains(path, "write-files") {
				return fmt.Errorf("write-files API error")
			}
			if strings.Contains(path, "import-file") {
				importFileCalled = true
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, cache.NewCache(1*time.Second))

	err := client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !importFileCalled {
		t.Error("Expected import-file fallback to be called")
	}
}

// TestReadDir verifies that ReadDir returns directory entries correctly
func TestReadDir(t *testing.T) {
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "list-files") {
				resp := response.(*listFilesResponse)
				resp.Objects = []wsfsObjectInfo{
					{
						ObjectInfo: workspace.ObjectInfo{
							Path:       "/test/file1.txt",
							ObjectType: workspace.ObjectTypeFile,
							Size:       100,
							ModifiedAt: time.Now().UnixMilli(),
						},
					},
					{
						ObjectInfo: workspace.ObjectInfo{
							Path:       "/test/file2.txt",
							ObjectType: workspace.ObjectTypeFile,
							Size:       200,
							ModifiedAt: time.Now().UnixMilli(),
						},
					},
					{
						ObjectInfo: workspace.ObjectInfo{
							Path:       "/test/subdir",
							ObjectType: workspace.ObjectTypeDirectory,
							ModifiedAt: time.Now().UnixMilli(),
						},
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	entries, err := client.ReadDir(context.Background(), "/test")
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// Verify entries are sorted by name
	expectedNames := []string{"file1.txt", "file2.txt", "subdir"}
	for i, entry := range entries {
		if entry.Name() != expectedNames[i] {
			t.Errorf("Expected entry[%d] name %q, got %q", i, expectedNames[i], entry.Name())
		}
	}

	// Verify file vs directory
	if entries[0].IsDir() || entries[1].IsDir() {
		t.Error("Files should not be directories")
	}
	if !entries[2].IsDir() {
		t.Error("Subdirectory should be a directory")
	}
}

// TestCacheInvalidation verifies that Write invalidates cache
func TestCacheInvalidation(t *testing.T) {
	statCallCount := 0
	testContent := []byte("new content")

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				statCallCount++
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       int64(len(testContent)),
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			if strings.Contains(path, "import-file") {
				return nil
			}
			if strings.Contains(path, "new-files") || strings.Contains(path, "write-files") {
				return fmt.Errorf("skip to fallback")
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, cache.NewCache(10*time.Second))

	// First Stat should call API
	_, err := client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if statCallCount != 1 {
		t.Errorf("Expected 1 Stat call, got %d", statCallCount)
	}

	// Second Stat should use cache
	_, err = client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if statCallCount != 1 {
		t.Errorf("Expected 1 Stat call (cached), got %d", statCallCount)
	}

	// Write should invalidate cache
	err = client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Third Stat should call API again (cache invalidated)
	_, err = client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if statCallCount != 2 {
		t.Errorf("Expected 2 Stat calls (cache invalidated), got %d", statCallCount)
	}
}

// TestWSFileInfoImplementsFileInfo verifies that WSFileInfo correctly implements fs.FileInfo
func TestWSFileInfoImplementsFileInfo(t *testing.T) {
	now := time.Now()
	info := WSFileInfo{
		ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/file.txt",
			ObjectType: workspace.ObjectTypeFile,
			Size:       1234,
			ModifiedAt: now.UnixMilli(),
		},
	}

	if info.Name() != "file.txt" {
		t.Errorf("Expected name 'file.txt', got %q", info.Name())
	}

	if info.Size() != 1234 {
		t.Errorf("Expected size 1234, got %d", info.Size())
	}

	if info.IsDir() {
		t.Error("File should not be a directory")
	}

	if info.Mode() != 0644 {
		t.Errorf("Expected mode 0644, got %o", info.Mode())
	}

	// ModTime should be close to now (within 1 second)
	modTime := info.ModTime()
	if modTime.Sub(now) > time.Second || now.Sub(modTime) > time.Second {
		t.Errorf("Expected ModTime close to %v, got %v", now, modTime)
	}
}

// TestWSFileInfoDirectory verifies that directory types work correctly
func TestWSFileInfoDirectory(t *testing.T) {
	dirInfo := WSFileInfo{
		ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/dir",
			ObjectType: workspace.ObjectTypeDirectory,
			ModifiedAt: time.Now().UnixMilli(),
		},
	}

	if !dirInfo.IsDir() {
		t.Error("Directory should be a directory")
	}

	if dirInfo.Mode()&fs.ModeDir == 0 {
		t.Error("Directory mode should include ModeDir flag")
	}

	repoInfo := WSFileInfo{
		ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/repo",
			ObjectType: workspace.ObjectTypeRepo,
			ModifiedAt: time.Now().UnixMilli(),
		},
	}

	if !repoInfo.IsDir() {
		t.Error("Repo should be treated as a directory")
	}
}

// TestHelperFunctions tests the helper functions
func TestHelperFunctions(t *testing.T) {
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") && strings.Contains(path, "file.txt") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test/file.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       100,
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			if strings.Contains(path, "object-info") && strings.Contains(path, "dir") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test/dir",
						ObjectType: workspace.ObjectTypeDirectory,
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			return fs.ErrNotExist
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	// Test Exists
	exists, err := client.Exists(context.Background(), "/test/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Error("File should exist")
	}

	exists, err = client.Exists(context.Background(), "/test/nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent file")
	}

	// Test IsFile
	isFile, err := client.IsFile(context.Background(), "/test/file.txt")
	if err != nil {
		t.Fatalf("IsFile failed: %v", err)
	}
	if !isFile {
		t.Error("Should be a file")
	}

	// Test IsDir
	isDir, err := client.IsDir(context.Background(), "/test/dir")
	if err != nil {
		t.Fatalf("IsDir failed: %v", err)
	}
	if !isDir {
		t.Error("Should be a directory")
	}

	isDir, err = client.IsDir(context.Background(), "/test/file.txt")
	if err != nil {
		t.Fatalf("IsDir failed: %v", err)
	}
	if isDir {
		t.Error("File should not be a directory")
	}
}

// TestInMemoryFileSystem tests the in-memory filesystem helper
func TestInMemoryFileSystem(t *testing.T) {
	fs := NewInMemoryFileSystem()

	// Test SetFile and GetFile
	fs.SetFile("/test.txt", []byte("content"))
	content, exists := fs.GetFile("/test.txt")
	if !exists {
		t.Error("File should exist")
	}
	if string(content) != "content" {
		t.Errorf("Expected content 'content', got %q", string(content))
	}

	// Test SetDir and IsDir
	fs.SetDir("/test")
	if !fs.IsDir("/test") {
		t.Error("Directory should exist")
	}

	// Test ListFiles
	fs.SetFile("/dir/file1.txt", []byte("1"))
	fs.SetFile("/dir/file2.txt", []byte("2"))
	fs.SetFile("/dir/nested/file3.txt", []byte("3"))
	fs.SetDir("/dir/subdir")

	files := fs.ListFiles("/dir")
	if len(files) != 3 { // file1.txt, file2.txt, subdir (not nested/file3.txt)
		t.Errorf("Expected 3 files in /dir, got %d: %v", len(files), files)
	}

	// Test Delete
	fs.Delete("/test.txt")
	_, exists = fs.GetFile("/test.txt")
	if exists {
		t.Error("File should be deleted")
	}
}

// Benchmark tests

func BenchmarkStatWithCache(b *testing.B) {
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			resp := response.(*objectInfoResponse)
			resp.WsfsObjectInfo = wsfsObjectInfo{
				ObjectInfo: workspace.ObjectInfo{
					Path:       "/test.txt",
					ObjectType: workspace.ObjectTypeFile,
					Size:       100,
					ModifiedAt: time.Now().UnixMilli(),
				},
			}
			return nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	// Prime the cache
	client.Stat(context.Background(), "/test.txt")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Stat(context.Background(), "/test.txt")
	}
}

func BenchmarkJSONEncoding(b *testing.B) {
	obj := wsfsObjectInfo{
		ObjectInfo: workspace.ObjectInfo{
			Path:       "/test/file.txt",
			ObjectType: workspace.ObjectTypeFile,
			Size:       1234,
			ModifiedAt: time.Now().UnixMilli(),
		},
		SignedURL: &struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers,omitempty"`
		}{
			URL:     "https://example.com/signed-url",
			Headers: map[string]string{"X-Custom": "value"},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(obj)
		if err != nil {
			b.Fatal(err)
		}
	}
}
