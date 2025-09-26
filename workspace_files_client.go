package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/workspace"
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
	Path string `json:"path"`
}

func NewListFilesRequest(path string) ListFilesRequest {
	return ListFilesRequest{Path: path}
}

type Object struct {
	ObjectInfo workspace.ObjectInfo `json:"object_info"`
}

type ListFilesResponse struct {
	Objects []Object `json:"objects,omitempty"`
}

func (wf *WorkspaceFilesClient) ListFiles(ctx context.Context, req ListFilesRequest) (*ListFilesResponse, error) {
	resp := &ListFilesResponse{}

	path := "/api/2.0/workspace-files/list-files"
	path += fmt.Sprintf("?path=%s", url.QueryEscape(req.Path))

	err := wf.databricksClient.Do(ctx, "GET", path, nil, nil, nil, resp)
	return resp, err
}

type ObjectInfoRequest struct {
	Path string `json:"Path"`
}

func NewObjectInfoRequest(path string) ObjectInfoRequest {
	return ObjectInfoRequest{Path: path}
}

type SingnedUrl struct {
	Url string `json:"url"`
}

type WsfsObjectInfo struct {
	ObjectInfo workspace.ObjectInfo `json:"object_info"`
	SingnedUrl SingnedUrl           `json:"signed_url"`
}

type ObjectInfoResponse struct {
	WsfsObjectInfo WsfsObjectInfo `json:"wsfs_object_info"`
}

func (wf *WorkspaceFilesClient) ObjectInfo(ctx context.Context, req ObjectInfoRequest) (*ObjectInfoResponse, error) {
	resp := &ObjectInfoResponse{}

	path := "/api/2.0/workspace-files/object-info"
	path += fmt.Sprintf("?path=%s", url.QueryEscape(req.Path))

	err := wf.databricksClient.Do(ctx, "GET", path, nil, nil, nil, resp)
	return resp, err
}
