package databricks

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/metacache"
)

type otherFileInfo struct{}

func (o otherFileInfo) Name() string       { return "other" }
func (o otherFileInfo) Size() int64        { return 0 }
func (o otherFileInfo) Mode() fs.FileMode  { return 0 }
func (o otherFileInfo) ModTime() time.Time { return time.Unix(0, 0) }
func (o otherFileInfo) IsDir() bool        { return false }
func (o otherFileInfo) Sys() any           { return nil }

func TestWSFileInfoSys(t *testing.T) {
	info := WSFileInfo{ObjectInfo: workspace.ObjectInfo{Path: "/x"}}
	sys := info.Sys()
	obj, ok := sys.(workspace.ObjectInfo)
	if !ok {
		t.Fatalf("expected ObjectInfo, got %T", sys)
	}
	if obj.Path != "/x" {
		t.Fatalf("unexpected path: %s", obj.Path)
	}
}

func TestWSDirEntryMethods(t *testing.T) {
	info := WSFileInfo{ObjectInfo: workspace.ObjectInfo{ObjectType: workspace.ObjectTypeNotebook, Path: "/nb"}}
	entry := WSDirEntry{WSFileInfo: info}
	if entry.Type() != entry.Mode() {
		t.Fatalf("expected Type to match Mode")
	}
	fi, err := entry.Info()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := fi.(WSFileInfo); !ok {
		t.Fatalf("expected WSFileInfo, got %T", fi)
	}
	if !entry.IsNotebook() {
		t.Fatal("expected notebook entry")
	}
}

func TestToWSFileInfo(t *testing.T) {
	if _, ok := toWSFileInfo(nil); ok {
		t.Fatal("expected false for nil")
	}
	if _, ok := toWSFileInfo(otherFileInfo{}); ok {
		t.Fatal("expected false for other type")
	}
	info := WSFileInfo{ObjectInfo: workspace.ObjectInfo{Path: "/x"}}
	got, ok := toWSFileInfo(info)
	if !ok {
		t.Fatal("expected true")
	}
	if got.Path != "/x" {
		t.Fatalf("unexpected path: %s", got.Path)
	}
}

func TestFakeWorkspaceAPIDefaults(t *testing.T) {
	api := &FakeWorkspaceAPI{}
	ctx := context.Background()

	if _, err := api.Stat(ctx, "/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
	if _, err := api.ReadDir(ctx, "/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
	if _, err := api.ReadAll(ctx, "/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
	if err := api.Write(ctx, "/file", []byte("data")); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if err := api.Delete(ctx, "/file", false); err != nil {
		t.Fatalf("unexpected delete error: %v", err)
	}
	if err := api.Mkdir(ctx, "/dir"); err != nil {
		t.Fatalf("unexpected mkdir error: %v", err)
	}
	if err := api.Rename(ctx, "/a", "/b"); err != nil {
		t.Fatalf("unexpected rename error: %v", err)
	}

	api.CacheSet("/file", nil)
	api.CacheInvalidate("/file")
}

func TestNewTestFileInfoHelpers(t *testing.T) {
	info := NewTestFileInfo("/dir", 10, true)
	if !info.IsDir() {
		t.Fatal("expected directory")
	}
	if info.Size() != 10 {
		t.Fatalf("unexpected size: %d", info.Size())
	}

	signed := NewTestFileInfoWithSignedURL("/file", 5, "http://example", map[string]string{"h": "v"})
	if signed.SignedURL != "http://example" {
		t.Fatalf("unexpected signed url: %s", signed.SignedURL)
	}
	if signed.SignedURLHeaders["h"] != "v" {
		t.Fatalf("unexpected headers: %+v", signed.SignedURLHeaders)
	}
}

func TestWorkspaceFilesClientCacheSetInvalidate(t *testing.T) {
	cache := metacache.NewCache(1 * time.Minute)
	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, &MockAPIClient{}, cache)

	info := NewTestFileInfo("/file", 1, false)
	client.CacheSet("/file", info)
	got, ok := cache.Get("/file")
	if !ok || got == nil {
		t.Fatal("expected cached entry")
	}

	client.CacheInvalidate("/file")
	if _, ok := cache.Get("/file"); ok {
		t.Fatal("expected cache invalidated")
	}
}

func TestWorkspaceFilesClientIsDirIsFileStatError(t *testing.T) {
	mockAPI := &MockAPIClient{
		DoFunc: func(ctx context.Context, method, path string,
			headers map[string]string, queryParams map[string]any, request, response any,
			visitors ...func(*http.Request) error) error {
			return fs.ErrNotExist
		},
	}
	client := NewWorkspaceFilesClientWithDeps(&MockWorkspaceClient{}, mockAPI, metacache.NewCache(1*time.Second))

	if ok, err := client.IsDir(context.Background(), "/missing"); err == nil || ok {
		t.Fatalf("expected IsDir error")
	}
	if ok, err := client.IsFile(context.Background(), "/missing"); err == nil || ok {
		t.Fatalf("expected IsFile error")
	}
}
