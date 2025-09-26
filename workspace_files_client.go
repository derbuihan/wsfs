package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"sort"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/workspace"
)

// WSFileInfo

type WSFileInfo struct {
	workspace.ObjectInfo
	SignedURL string
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
		URL string `json:"url"`
	} `json:"signed_url,omitempty"`
}

type listFilesResponse struct {
	Objects []wsfsObjectInfo `json:"objects"`
}

type objectInfoResponse struct {
	WsfsObjectInfo wsfsObjectInfo `json:"wsfs_object_info"`
}

// WorkspaceFilesClient

type WorkspaceFilesClient struct {
	workspaceClient *databricks.WorkspaceClient
	apiClient       *client.DatabricksClient
}

func NewWorkspaceFilesClient(w *databricks.WorkspaceClient) (*WorkspaceFilesClient, error) {
	databricksClient, err := client.New(w.Config)
	if err != nil {
		return nil, err
	}

	return &WorkspaceFilesClient{
		workspaceClient: w,
		apiClient:       databricksClient,
	}, nil
}

func (c *WorkspaceFilesClient) Stat(ctx context.Context, filePath string) (fs.FileInfo, error) {
	var resp objectInfoResponse

	urlPath := fmt.Sprintf(
		"/api/2.0/workspace-files/object-info?path=%s",
		url.QueryEscape(filePath),
	)

	err := c.apiClient.Do(ctx, http.MethodGet, urlPath, nil, nil, nil, &resp)
	if err != nil {
		return nil, err
	}

	info := WSFileInfo{
		ObjectInfo: resp.WsfsObjectInfo.ObjectInfo,
	}
	if resp.WsfsObjectInfo.SignedURL != nil {
		info.SignedURL = resp.WsfsObjectInfo.SignedURL.URL
	}

	return info, nil
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
		}
		entries[i] = WSDirEntry{info}
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	return entries, nil
}

func (c *WorkspaceFilesClient) Read(ctx context.Context, filePath string) (io.ReadCloser, error) {
	return c.workspaceClient.Workspace.Download(ctx, filePath)
}

func (c *WorkspaceFilesClient) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	resp, err := c.workspaceClient.Workspace.Export(ctx, workspace.ExportRequest{
		Path:   filePath,
		Format: workspace.ExportFormatSource,
	})
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(resp.Content)
}
