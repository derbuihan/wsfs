package databricks

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/cache"
	"wsfs/internal/logging"
)

// WSFileInfo

type WSFileInfo struct {
	workspace.ObjectInfo
	SignedURL        string
	SignedURLHeaders map[string]string
}

func (info WSFileInfo) Name() string {
	return path.Base(info.Path)
}

func (info WSFileInfo) Size() int64 {
	return info.ObjectInfo.Size
}

func (info WSFileInfo) Mode() fs.FileMode {
	switch info.ObjectType {
	case workspace.ObjectTypeDirectory, workspace.ObjectTypeRepo:
		return fs.ModeDir | 0755
	default:
		return 0644
	}
}

func (info WSFileInfo) ModTime() time.Time {
	return time.UnixMilli(info.ModifiedAt)
}

func (info WSFileInfo) IsDir() bool {
	return info.ObjectType == workspace.ObjectTypeDirectory || info.ObjectType == workspace.ObjectTypeRepo
}

func (info WSFileInfo) Sys() any {
	return info.ObjectInfo
}

// WSDirEntry

type WSDirEntry struct {
	WSFileInfo
}

func (entry WSDirEntry) Type() fs.FileMode {
	return entry.Mode()
}

func (entry WSDirEntry) Info() (fs.FileInfo, error) {
	return entry.WSFileInfo, nil
}

// workspace-files

type wsfsObjectInfo struct {
	ObjectInfo workspace.ObjectInfo `json:"object_info"`
	SignedURL  *struct {
		URL     string            `json:"url"`
		Headers map[string]string `json:"headers,omitempty"`
	} `json:"signed_url,omitempty"`
}

type listFilesResponse struct {
	Objects []wsfsObjectInfo `json:"objects"`
}

type objectInfoResponse struct {
	WsfsObjectInfo wsfsObjectInfo `json:"wsfs_object_info"`
}

// WorkspaceFilesClient

type apiDoer interface {
	Do(ctx context.Context, method, path string,
		headers map[string]string, queryParams map[string]any, request, response any,
		visitors ...func(*http.Request) error) error
}

// workspaceClient is a thin interface that defines only the methods we need from workspace.WorkspaceInterface
// This makes testing easier without having to implement the entire interface
type workspaceClient interface {
	Export(ctx context.Context, request workspace.ExportRequest) (*workspace.ExportResponse, error)
	Delete(ctx context.Context, request workspace.Delete) error
	Mkdirs(ctx context.Context, request workspace.Mkdirs) error
}

type WorkspaceFilesClient struct {
	workspaceClient workspaceClient
	apiClient       apiDoer
	cache           *cache.Cache
}

func NewWorkspaceFilesClient(w *databricks.WorkspaceClient) (*WorkspaceFilesClient, error) {
	databricksClient, err := client.New(w.Config)
	if err != nil {
		return nil, err
	}

	return NewWorkspaceFilesClientWithDeps(w.Workspace, databricksClient, nil), nil
}

func NewWorkspaceFilesClientWithDeps(workspaceClient workspaceClient, apiClient apiDoer, c *cache.Cache) *WorkspaceFilesClient {
	if c == nil {
		c = cache.NewCache(60 * time.Second)
	}
	return &WorkspaceFilesClient{
		workspaceClient: workspaceClient,
		apiClient:       apiClient,
		cache:           c,
	}
}

func (c *WorkspaceFilesClient) Stat(ctx context.Context, filePath string) (fs.FileInfo, error) {
	info, found := c.cache.Get(filePath)
	if found {
		if info == nil {
			return nil, fs.ErrNotExist
		}
		return info, nil
	}

	var resp objectInfoResponse
	urlPath := fmt.Sprintf(
		"/api/2.0/workspace-files/object-info?path=%s",
		url.QueryEscape(filePath),
	)

	err := c.apiClient.Do(ctx, http.MethodGet, urlPath, nil, nil, nil, &resp)
	if err != nil {
		c.cache.Set(filePath, nil)
		return nil, err
	}

	apiInfo := WSFileInfo{ObjectInfo: resp.WsfsObjectInfo.ObjectInfo}
	if resp.WsfsObjectInfo.SignedURL != nil {
		apiInfo.SignedURL = resp.WsfsObjectInfo.SignedURL.URL
		apiInfo.SignedURLHeaders = resp.WsfsObjectInfo.SignedURL.Headers
	}
	c.cache.Set(filePath, apiInfo)
	return apiInfo, nil
}

func (c *WorkspaceFilesClient) ReadDir(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
	var resp listFilesResponse

	urlPath := fmt.Sprintf(
		"/api/2.0/workspace-files/list-files?path=%s",
		url.QueryEscape(dirPath),
	)

	if err := c.apiClient.Do(ctx, http.MethodGet, urlPath, nil, nil, nil, &resp); err != nil {
		return nil, err
	}

	entries := make([]fs.DirEntry, len(resp.Objects))
	for i, obj := range resp.Objects {
		info := WSFileInfo{
			ObjectInfo: obj.ObjectInfo,
		}
		if obj.SignedURL != nil {
			info.SignedURL = obj.SignedURL.URL
			info.SignedURLHeaders = obj.SignedURL.Headers
		}
		entries[i] = WSDirEntry{info}
		c.cache.Set(info.Path, info)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	return entries, nil
}

func (c *WorkspaceFilesClient) readViaSignedURL(ctx context.Context, url string, headers map[string]string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	// Set signed URL headers
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("signed URL GET failed with status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *WorkspaceFilesClient) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	// 1. Get signed URL from object-info (may already be cached in Stat())
	info, err := c.Stat(ctx, filePath)
	if err != nil {
		return nil, err
	}

	wsInfo := info.(WSFileInfo)

	// 2. Try to use signed URL if available
	if wsInfo.SignedURL != "" {
		data, err := c.readViaSignedURL(ctx, wsInfo.SignedURL, wsInfo.SignedURLHeaders)
		if err == nil {
			logging.Debugf("Read via signed URL succeeded for path: %s", filePath)
			return data, nil
		}
		logging.Debugf("Read via signed URL failed for path: %s, falling back to Export: %v", filePath, err)
	}

	// 3. Fallback: workspace.Export
	resp, err := c.workspaceClient.Export(ctx, workspace.ExportRequest{
		Path:   filePath,
		Format: workspace.ExportFormatSource,
	})
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(resp.Content)
}

func (c *WorkspaceFilesClient) writeViaNewFiles(ctx context.Context, filepath string, data []byte) error {
	// 1. Call new-files API to get signed URL
	contentB64 := base64.StdEncoding.EncodeToString(data)
	reqBody := map[string]any{
		"path":    filepath,
		"content": contentB64,
	}

	var resp struct {
		SignedURLs []struct {
			URL     string            `json:"url"`
			Headers map[string]string `json:"headers"`
		} `json:"signed_urls"`
	}

	err := c.apiClient.Do(ctx, http.MethodPost, "/api/2.0/workspace-files/new-files", nil, nil, reqBody, &resp)
	if err != nil {
		return err
	}

	if len(resp.SignedURLs) == 0 {
		return fmt.Errorf("no signed URL returned")
	}

	// 2. Upload to signed URL with PUT
	signedURL := resp.SignedURLs[0]
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL.URL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	for k, v := range signedURL.Headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	putResp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("signed URL PUT failed with status %d: %s", putResp.StatusCode, string(body))
	}

	return nil
}

func (c *WorkspaceFilesClient) writeViaWriteFiles(ctx context.Context, filepath string, data []byte) error {
	contentB64 := base64.StdEncoding.EncodeToString(data)
	reqBody := map[string]any{
		"files": []map[string]any{
			{
				"path":      filepath,
				"content":   contentB64,
				"overwrite": true,
			},
		},
	}

	return c.apiClient.Do(ctx, http.MethodPost, "/api/2.0/workspace-files/write-files", nil, nil, reqBody, nil)
}

func (c *WorkspaceFilesClient) Write(ctx context.Context, filepath string, data []byte) error {
	c.cache.Invalidate(filepath)

	// 1. Try new-files (experimental)
	err := c.writeViaNewFiles(ctx, filepath, data)
	if err == nil {
		logging.Debugf("Write via new-files succeeded for path: %s", filepath)
		return nil
	}
	logging.Debugf("Write via new-files failed for path: %s, trying write-files: %v", filepath, err)

	// 2. Try write-files (experimental)
	err = c.writeViaWriteFiles(ctx, filepath, data)
	if err == nil {
		logging.Debugf("Write via write-files succeeded for path: %s", filepath)
		return nil
	}
	logging.Debugf("Write via write-files failed for path: %s, falling back to import-file: %v", filepath, err)

	// 3. Fallback: import-file
	urlPath := fmt.Sprintf(
		"/api/2.0/workspace-files/import-file/%s?overwrite=true",
		url.PathEscape(strings.TrimLeft(filepath, "/")),
	)

	return c.apiClient.Do(ctx, http.MethodPost, urlPath, nil, nil, data, nil)
}

func (c *WorkspaceFilesClient) Delete(ctx context.Context, filePath string, recursive bool) error {
	c.cache.Invalidate(filePath)

	return c.workspaceClient.Delete(ctx, workspace.Delete{
		Path:      filePath,
		Recursive: recursive,
	})
}

func (c *WorkspaceFilesClient) Mkdir(ctx context.Context, dirPath string) error {
	c.cache.Invalidate(dirPath)

	return c.workspaceClient.Mkdirs(ctx, workspace.Mkdirs{
		Path: dirPath,
	})
}

func (c *WorkspaceFilesClient) Rename(ctx context.Context, source_path string, destination_path string) error {
	urlPath := "/api/2.0/workspace/rename"

	reqBody := map[string]any{
		"source_path":      source_path,
		"destination_path": destination_path,
	}

	err := c.apiClient.Do(ctx, http.MethodPost, urlPath, nil, nil, reqBody, nil)
	if err != nil {
		return err
	}

	c.cache.Invalidate(source_path)
	c.cache.Invalidate(destination_path)
	return nil
}

// Helpers

func (c *WorkspaceFilesClient) CacheSet(filePath string, info fs.FileInfo) {
	c.cache.Set(filePath, info)
}

func (c *WorkspaceFilesClient) Exists(ctx context.Context, path string) (bool, error) {
	_, err := c.Stat(ctx, path)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *WorkspaceFilesClient) IsDir(ctx context.Context, path string) (bool, error) {
	stat, err := c.Stat(ctx, path)
	if err != nil {
		return false, err
	}
	return stat.IsDir(), nil
}

func (c *WorkspaceFilesClient) IsFile(ctx context.Context, path string) (bool, error) {
	stat, err := c.Stat(ctx, path)
	if err != nil {
		return false, err
	}
	return !stat.IsDir(), nil
}
