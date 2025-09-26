package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
)

type WorkspaceFilesClient struct {
	workspaceClient  *databricks.WorkspaceClient
	databricksClient *client.DatabricksClient
}

func NewWorkspaceFilesClient(w *databricks.WorkspaceClient) (*WorkspaceFilesClient, error) {
	databricksClient, err := client.New(w.Config)
	if err != nil {
		return nil, err
	}

	return &WorkspaceFilesClient{
		workspaceClient:  w,
		databricksClient: databricksClient,
	}, nil
}

type ListFilesRequest struct {
	Path        string `json:"path"`
	IncludeSize *bool  `json:"include_size,omitempty"`
	Recursive   *bool  `json:"recursive,omitempty"`
}

func NewListFilesRequest(path string) ListFilesRequest {
	return ListFilesRequest{
		Path:        path,
		IncludeSize: nil,
		Recursive:   nil,
	}
}

type ObjectInfo struct {
	ObjectType         string `json:"object_type,omitempty"`
	Path               string `json:"path,omitempty"`
	Language           string `json:"language,omitempty"`
	CreatedAt          int64  `json:"created_at,omitempty"`
	ModifiedAt         int64  `json:"modified_at,omitempty"`
	ObjectID           int64  `json:"object_id,omitempty"`
	BlobPath           string `json:"blob_path,omitempty"`
	ContentSHA256Hex   string `json:"content_sha256_hex,omitempty"`
	Size               int64  `json:"size,omitempty"`
	BlobLocation       string `json:"blob_location,omitempty"`
	HasWsfsMetadata    bool   `json:"has_wsfs_metadata,omitempty"`
	RequiresSyncToWsfs bool   `json:"requires_sync_to_wsfs,omitempty"`
	ResourceID         string `json:"resource_id,omitempty"`
}

type Object struct {
	ObjectInfo ObjectInfo `json:"object_info"`
}

type ListFilesResponse struct {
	Objects []Object `json:"objects,omitempty"`
}

func (wf *WorkspaceFilesClient) ListFiles(
	ctx context.Context,
	req ListFilesRequest,
) (*ListFilesResponse, error) {
	resp := &ListFilesResponse{}

	path := "/api/2.0/workspace-files/list-files"
	path += fmt.Sprintf("?path=%s", url.QueryEscape(req.Path))

	err := wf.databricksClient.Do(ctx, "GET", path, nil, nil, nil, resp)
	return resp, err
}
