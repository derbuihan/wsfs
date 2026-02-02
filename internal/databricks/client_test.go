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

	"wsfs/internal/metacache"
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

// TestReadAllViaSignedURL verifies that ReadAll uses signed URL for large files (>= 5MB)
func TestReadAllViaSignedURL(t *testing.T) {
	// Create a large file (>= 5MB threshold)
	testContent := make([]byte, 5*1024*1024) // 5MB
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}

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

	if len(data) != len(testContent) {
		t.Errorf("Expected content length %d, got %d", len(testContent), len(data))
	}
}

// TestReadAllFallbackToExport verifies that ReadAll falls back to Export when signed URL fails for large files
func TestReadAllFallbackToExport(t *testing.T) {
	// Create a large file (>= 5MB threshold) to test fallback path
	testContent := make([]byte, 5*1024*1024) // 5MB
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}
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
		t.Error("Expected signed URL to be called for large file")
	}

	if !exportCalled {
		t.Error("Expected Export fallback to be called")
	}

	if len(data) != len(testContent) {
		t.Errorf("Expected content length %d, got %d", len(testContent), len(data))
	}
}

// TestReadSmallFilesUseExport verifies that small files (< 5MB) use Export directly
func TestReadSmallFilesUseExport(t *testing.T) {
	testContent := []byte("small test content") // Much smaller than 5MB threshold
	contentB64 := base64.StdEncoding.EncodeToString(testContent)

	signedURLCalled := false
	exportCalled := false

	// Set up a test HTTP server (should NOT be called)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signedURLCalled = true
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

	if signedURLCalled {
		t.Error("Signed URL should NOT be called for small files")
	}

	if !exportCalled {
		t.Error("Expected Export to be called for small files")
	}

	if string(data) != string(testContent) {
		t.Errorf("Expected content %q, got %q", string(testContent), string(data))
	}
}

// TestWriteViaNewFiles verifies that Write uses new-files API for large files (>= 5MB)
func TestWriteViaNewFiles(t *testing.T) {
	// Create a large file (>= 5MB threshold)
	testContent := make([]byte, 5*1024*1024) // 5MB
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}
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

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(1*time.Second))

	err := client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !signedURLCalled {
		t.Error("Expected signed URL PUT to be called")
	}
}

// TestWriteFallbackToImportFile verifies that Write falls back to import-file for large files
func TestWriteFallbackToImportFile(t *testing.T) {
	// Create a large file (>= 5MB threshold) to test fallback path
	testContent := make([]byte, 5*1024*1024) // 5MB
	for i := range testContent {
		testContent[i] = byte(i % 256)
	}
	importFileCalled := false

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "new-files") {
				return fmt.Errorf("new-files API error")
			}
			if strings.Contains(path, "import-file") {
				importFileCalled = true
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(1*time.Second))

	err := client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if !importFileCalled {
		t.Error("Expected import-file fallback to be called")
	}
}

// TestWriteSmallFilesUseImportFile verifies that small files (< 5MB) use import-file directly
func TestWriteSmallFilesUseImportFile(t *testing.T) {
	testContent := []byte("small test content") // Much smaller than 5MB threshold
	importFileCalled := false
	newFilesCalled := false

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "new-files") {
				newFilesCalled = true
				return fmt.Errorf("new-files should not be called for small files")
			}
			if strings.Contains(path, "import-file") {
				importFileCalled = true
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(1*time.Second))

	err := client.Write(context.Background(), "/test.txt", testContent)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	if newFilesCalled {
		t.Error("new-files should NOT be called for small files")
	}
	if !importFileCalled {
		t.Error("Expected import-file to be called for small files")
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
			if strings.Contains(path, "new-files") {
				return fmt.Errorf("skip to fallback")
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(10*time.Second))

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

// TestDelete verifies that Delete calls the workspace client and invalidates cache
func TestDelete(t *testing.T) {
	deleteCalled := false
	var deletedPath string
	var deleteRecursive bool

	mockWorkspace := &MockWorkspaceClient{
		DeleteFunc: func(ctx context.Context, request workspace.Delete) error {
			deleteCalled = true
			deletedPath = request.Path
			deleteRecursive = request.Recursive
			return nil
		},
	}

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
						Size:       100,
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, mockAPI, metacache.NewCache(10*time.Second))

	// Prime the cache
	_, err := client.Stat(context.Background(), "/test.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Verify cache hit
	_, found := client.cache.Get("/test.txt")
	if !found {
		t.Error("Expected cache entry before delete")
	}

	// Delete the file
	err = client.Delete(context.Background(), "/test.txt", false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if !deleteCalled {
		t.Error("Expected Delete to be called on workspace client")
	}
	if deletedPath != "/test.txt" {
		t.Errorf("Expected path '/test.txt', got %q", deletedPath)
	}
	if deleteRecursive {
		t.Error("Expected recursive to be false")
	}

	// Verify cache was invalidated
	_, found = client.cache.Get("/test.txt")
	if found {
		t.Error("Expected cache entry to be invalidated after delete")
	}
}

// TestMkdir verifies that Mkdir calls the workspace client and invalidates cache
func TestMkdir(t *testing.T) {
	mkdirCalled := false
	var createdPath string

	mockWorkspace := &MockWorkspaceClient{
		MkdirsFunc: func(ctx context.Context, request workspace.Mkdirs) error {
			mkdirCalled = true
			createdPath = request.Path
			return nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, &MockAPIClient{}, metacache.NewCache(10*time.Second))

	err := client.Mkdir(context.Background(), "/newdir")
	if err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	if !mkdirCalled {
		t.Error("Expected Mkdirs to be called on workspace client")
	}
	if createdPath != "/newdir" {
		t.Errorf("Expected path '/newdir', got %q", createdPath)
	}
}

// TestRename verifies that Rename invalidates cache for both source and destination
func TestRename(t *testing.T) {
	renameCalled := false
	var sourcePath, destPath string

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/old.txt",
						ObjectType: workspace.ObjectTypeFile,
						Size:       100,
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			if strings.Contains(path, "rename") {
				renameCalled = true
				reqMap := request.(map[string]any)
				sourcePath = reqMap["source_path"].(string)
				destPath = reqMap["destination_path"].(string)
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(10*time.Second))

	// Prime cache for source file
	_, err := client.Stat(context.Background(), "/old.txt")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	// Verify cache hit
	_, found := client.cache.Get("/old.txt")
	if !found {
		t.Error("Expected cache entry before rename")
	}

	// Rename the file
	err = client.Rename(context.Background(), "/old.txt", "/new.txt")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}

	if !renameCalled {
		t.Error("Expected rename API to be called")
	}
	if sourcePath != "/old.txt" {
		t.Errorf("Expected source path '/old.txt', got %q", sourcePath)
	}
	if destPath != "/new.txt" {
		t.Errorf("Expected dest path '/new.txt', got %q", destPath)
	}

	// Verify both old and new paths are invalidated
	_, found = client.cache.Get("/old.txt")
	if found {
		t.Error("Expected old path cache entry to be invalidated")
	}
	_, found = client.cache.Get("/new.txt")
	if found {
		t.Error("Expected new path cache entry to be invalidated")
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

// ============================================================================
// Notebook (.ipynb) Tests
// ============================================================================

// TestIsNotebook verifies IsNotebook returns correct values for different ObjectTypes
func TestIsNotebook(t *testing.T) {
	tests := []struct {
		name       string
		objectType workspace.ObjectType
		expected   bool
	}{
		{"notebook", workspace.ObjectTypeNotebook, true},
		{"file", workspace.ObjectTypeFile, false},
		{"directory", workspace.ObjectTypeDirectory, false},
		{"repo", workspace.ObjectTypeRepo, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: tt.objectType}}
			if got := info.IsNotebook(); got != tt.expected {
				t.Errorf("IsNotebook() = %v, want %v", got, tt.expected)
			}
		})
	}
}

// TestStatWithIpynbSuffix verifies that Stat handles .ipynb suffix correctly
func TestStatWithIpynbSuffix(t *testing.T) {
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				// Return notebook info for the base path (without .ipynb)
				if strings.Contains(path, "notebook") && !strings.Contains(path, ".ipynb") {
					resp := response.(*objectInfoResponse)
					resp.WsfsObjectInfo = wsfsObjectInfo{
						ObjectInfo: workspace.ObjectInfo{
							Path:       "/test/notebook",
							ObjectType: workspace.ObjectTypeNotebook,
							Size:       100,
							ModifiedAt: time.Now().UnixMilli(),
						},
					}
					return nil
				}
			}
			return fs.ErrNotExist
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	// Test 1: Stat with .ipynb suffix should find the notebook
	info, err := client.Stat(context.Background(), "/test/notebook.ipynb")
	if err != nil {
		t.Fatalf("Stat with .ipynb suffix failed: %v", err)
	}
	wsInfo := info.(WSFileInfo)
	if !wsInfo.IsNotebook() {
		t.Error("Expected IsNotebook() to be true")
	}
	if wsInfo.Path != "/test/notebook" {
		t.Errorf("Expected path /test/notebook, got %s", wsInfo.Path)
	}

	// Test 2: Stat with .ipynb suffix for non-notebook should fail
	_, err = client.Stat(context.Background(), "/test/file.ipynb")
	if err == nil {
		t.Error("Expected error for .ipynb suffix on non-existent notebook")
	}
}

// TestReadAllNotebook verifies that ReadAll exports notebooks in JUPYTER format
func TestReadAllNotebook(t *testing.T) {
	notebookContent := `{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":4}`
	exportCalled := false
	exportFormat := workspace.ExportFormatSource

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				resp := response.(*objectInfoResponse)
				resp.WsfsObjectInfo = wsfsObjectInfo{
					ObjectInfo: workspace.ObjectInfo{
						Path:       "/test/notebook",
						ObjectType: workspace.ObjectTypeNotebook,
						Size:       int64(len(notebookContent)),
						ModifiedAt: time.Now().UnixMilli(),
					},
				}
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	mockWorkspace := &MockWorkspaceClient{
		ExportFunc: func(ctx context.Context, req workspace.ExportRequest) (*workspace.ExportResponse, error) {
			exportCalled = true
			exportFormat = req.Format
			return &workspace.ExportResponse{
				Content: base64.StdEncoding.EncodeToString([]byte(notebookContent)),
			}, nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, mockAPI, nil)

	data, err := client.ReadAll(context.Background(), "/test/notebook.ipynb")
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !exportCalled {
		t.Error("Expected Export to be called for notebook")
	}
	if exportFormat != workspace.ExportFormatJupyter {
		t.Errorf("Expected JUPYTER format, got %v", exportFormat)
	}
	if string(data) != notebookContent {
		t.Errorf("Expected %q, got %q", notebookContent, string(data))
	}
}

// TestWriteNotebook verifies that Write uses Import API with JUPYTER format for notebooks
func TestWriteNotebook(t *testing.T) {
	notebookContent := `{"cells":[{"cell_type":"code","source":["print('hello')"]}],"metadata":{},"nbformat":4,"nbformat_minor":4}`
	importCalled := false
	var importedPath string
	var importedFormat workspace.ImportFormat

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "object-info") {
				// Return notebook for existing path
				if strings.Contains(path, "existing_notebook") {
					resp := response.(*objectInfoResponse)
					resp.WsfsObjectInfo = wsfsObjectInfo{
						ObjectInfo: workspace.ObjectInfo{
							Path:       "/test/existing_notebook",
							ObjectType: workspace.ObjectTypeNotebook,
							Size:       100,
							ModifiedAt: time.Now().UnixMilli(),
						},
					}
					return nil
				}
			}
			return fs.ErrNotExist
		},
	}

	mockWorkspace := &MockWorkspaceClient{
		ImportFunc: func(ctx context.Context, req workspace.Import) error {
			importCalled = true
			importedPath = req.Path
			importedFormat = req.Format
			return nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, mockAPI, nil)

	// Test 1: Write to existing notebook
	err := client.Write(context.Background(), "/test/existing_notebook.ipynb", []byte(notebookContent))
	if err != nil {
		t.Fatalf("Write to existing notebook failed: %v", err)
	}
	if !importCalled {
		t.Error("Expected Import to be called for notebook")
	}
	if importedFormat != workspace.ImportFormatJupyter {
		t.Errorf("Expected JUPYTER format, got %v", importedFormat)
	}
	if importedPath != "/test/existing_notebook" {
		t.Errorf("Expected path without .ipynb suffix, got %s", importedPath)
	}

	// Test 2: Write new .ipynb file (should create as notebook)
	importCalled = false
	err = client.Write(context.Background(), "/test/new_notebook.ipynb", []byte(notebookContent))
	if err != nil {
		t.Fatalf("Write new .ipynb failed: %v", err)
	}
	if !importCalled {
		t.Error("Expected Import to be called for new .ipynb file")
	}
	if importedPath != "/test/new_notebook" {
		t.Errorf("Expected path without .ipynb suffix, got %s", importedPath)
	}
}

// TestDeleteNotebook verifies that Delete strips .ipynb suffix
func TestDeleteNotebook(t *testing.T) {
	var deletedPath string

	mockWorkspace := &MockWorkspaceClient{
		DeleteFunc: func(ctx context.Context, req workspace.Delete) error {
			deletedPath = req.Path
			return nil
		},
	}

	client := NewWorkspaceFilesClientWithDeps(mockWorkspace, &MockAPIClient{}, nil)

	err := client.Delete(context.Background(), "/test/notebook.ipynb", false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if deletedPath != "/test/notebook" {
		t.Errorf("Expected path without .ipynb suffix, got %s", deletedPath)
	}
}

// TestRenameNotebook verifies that Rename strips .ipynb suffix from both paths
func TestRenameNotebook(t *testing.T) {
	var sourcePathUsed, destPathUsed string

	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			if strings.Contains(path, "rename") {
				reqMap := request.(map[string]any)
				sourcePathUsed = reqMap["source_path"].(string)
				destPathUsed = reqMap["destination_path"].(string)
				return nil
			}
			return fmt.Errorf("unexpected path: %s", path)
		},
	}

	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, nil)

	err := client.Rename(context.Background(), "/test/old.ipynb", "/test/new.ipynb")
	if err != nil {
		t.Fatalf("Rename failed: %v", err)
	}
	if sourcePathUsed != "/test/old" {
		t.Errorf("Expected source path without .ipynb, got %s", sourcePathUsed)
	}
	if destPathUsed != "/test/new" {
		t.Errorf("Expected dest path without .ipynb, got %s", destPathUsed)
	}
}

// TestSanitizeURL verifies URL sanitization removes query parameters
func TestSanitizeURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "URL with query params",
			input:    "https://storage.example.com/bucket/file?sig=secret123&token=abc",
			expected: "https://storage.example.com/bucket/file",
		},
		{
			name:     "URL without query params",
			input:    "https://storage.example.com/bucket/file",
			expected: "https://storage.example.com/bucket/file",
		},
		{
			name:     "URL with fragment",
			input:    "https://example.com/page#section",
			expected: "https://example.com/page",
		},
		{
			name:     "Invalid URL",
			input:    "://invalid",
			expected: "[invalid URL]",
		},
		{
			name:     "URL with port",
			input:    "https://example.com:8080/path?token=secret",
			expected: "https://example.com:8080/path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeURL(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeURL(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestSanitizeError verifies error message sanitization
func TestSanitizeError(t *testing.T) {
	tests := []struct {
		name     string
		input    error
		contains []string    // strings that should be present
		excludes []string    // strings that should NOT be present
	}{
		{
			name:     "nil error",
			input:    nil,
			contains: []string{""},
		},
		{
			name:     "simple error without URL",
			input:    fmt.Errorf("connection timeout"),
			contains: []string{"connection timeout"},
		},
		{
			name:     "error with URL containing token",
			input:    fmt.Errorf("GET https://storage.example.com/file?sig=SECRET_TOKEN&exp=123 failed"),
			contains: []string{"GET", "storage.example.com/file", "failed"},
			excludes: []string{"SECRET_TOKEN", "sig=", "exp="},
		},
		{
			name:     "error with multiple URLs",
			input:    fmt.Errorf("redirect from https://a.com?t=1 to https://b.com?t=2"),
			contains: []string{"redirect", "from", "to"},
			excludes: []string{"t=1", "t=2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeError(tt.input)
			for _, s := range tt.contains {
				if !strings.Contains(result, s) {
					t.Errorf("sanitizeError() = %q, should contain %q", result, s)
				}
			}
			for _, s := range tt.excludes {
				if strings.Contains(result, s) {
					t.Errorf("sanitizeError() = %q, should NOT contain %q", result, s)
				}
			}
		})
	}
}

// TestTruncateBody verifies body truncation
func TestTruncateBody(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		maxLen   int
		expected string
	}{
		{
			name:     "short body",
			body:     "short",
			maxLen:   100,
			expected: "short",
		},
		{
			name:     "exact length",
			body:     "exact",
			maxLen:   5,
			expected: "exact",
		},
		{
			name:     "long body",
			body:     "this is a very long body that needs truncation",
			maxLen:   10,
			expected: "this is a ...[truncated]",
		},
		{
			name:     "empty body",
			body:     "",
			maxLen:   100,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateBody(tt.body, tt.maxLen)
			if result != tt.expected {
				t.Errorf("truncateBody(%q, %d) = %q, want %q", tt.body, tt.maxLen, result, tt.expected)
			}
		})
	}
}
