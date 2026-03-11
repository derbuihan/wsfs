package databricks

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/client"
	"github.com/databricks/databricks-sdk-go/service/workspace"

	"wsfs/internal/logging"
	"wsfs/internal/metacache"
	"wsfs/internal/pathutil"
	"wsfs/internal/retry"
)

// HTTP client timeout for signed URL operations
const httpTimeout = 2 * time.Minute

// Maximum length for response body in error messages
const maxErrorBodyLen = 200

// Size threshold for API selection (5MB)
// Files smaller than this use import-file directly (1 round trip)
// Files larger than this use new-files + signed URL (direct cloud storage)
const sizeThresholdForSignedURL = 5 * 1024 * 1024 // 5MB

const (
	defaultMetadataTTL = 10 * time.Second
	defaultNegativeTTL = 3 * time.Second
)

type CacheConfig struct {
	MetadataTTL time.Duration
	NegativeTTL time.Duration
}

func (c CacheConfig) withDefaults() CacheConfig {
	if c.MetadataTTL <= 0 {
		c.MetadataTTL = defaultMetadataTTL
	}
	if c.NegativeTTL <= 0 {
		c.NegativeTTL = defaultNegativeTTL
	}
	return c
}

// sanitizeURL removes query parameters from a URL to avoid exposing signed tokens
func sanitizeURL(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "[invalid URL]"
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

// sanitizeError removes sensitive information from error messages
func sanitizeError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	// Remove potential URLs with tokens (look for https:// patterns)
	// This is a simple heuristic - URLs in error messages often contain signed tokens
	if strings.Contains(msg, "https://") || strings.Contains(msg, "http://") {
		// Try to find and sanitize URLs in the message
		words := strings.Fields(msg)
		for i, word := range words {
			if strings.HasPrefix(word, "http://") || strings.HasPrefix(word, "https://") {
				words[i] = sanitizeURL(word)
			}
		}
		return strings.Join(words, " ")
	}
	return msg
}

// truncateBody truncates a response body for safe logging
func truncateBody(body string, maxLen int) string {
	if len(body) <= maxLen {
		return body
	}
	return body[:maxLen] + "...[truncated]"
}

func normalizeNotExistError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, fs.ErrNotExist) || apierr.IsMissing(err) {
		return fs.ErrNotExist
	}
	return err
}

// WSFileInfo

type WSFileInfo struct {
	workspace.ObjectInfo
	SignedURL        string
	SignedURLHeaders map[string]string
	// NotebookSizeComputed tracks whether Size reflects the exported notebook content.
	NotebookSizeComputed bool
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

func (info WSFileInfo) IsNotebook() bool {
	return info.ObjectType == workspace.ObjectTypeNotebook
}

func (info WSFileInfo) Sys() any {
	return info.ObjectInfo
}

// toWSFileInfo safely converts fs.FileInfo to WSFileInfo.
// Returns zero value and false if info is nil or not a WSFileInfo.
func toWSFileInfo(info fs.FileInfo) (WSFileInfo, bool) {
	if info == nil {
		return WSFileInfo{}, false
	}
	wsInfo, ok := info.(WSFileInfo)
	return wsInfo, ok
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

func (entry WSDirEntry) IsNotebook() bool {
	return entry.WSFileInfo.IsNotebook()
}

type visibleDirEntry struct {
	name string
	info WSFileInfo
}

func (entry visibleDirEntry) Name() string {
	return entry.name
}

func (entry visibleDirEntry) IsDir() bool {
	return entry.info.IsDir()
}

func (entry visibleDirEntry) Type() fs.FileMode {
	return entry.info.Mode()
}

func (entry visibleDirEntry) Info() (fs.FileInfo, error) {
	return entry.info, nil
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
	Import(ctx context.Context, request workspace.Import) error
	Upload(ctx context.Context, path string, r io.Reader, opts ...workspace.UploadOption) error
}

type WorkspaceFilesClient struct {
	workspaceClient workspaceClient
	apiClient       apiDoer
	cache           *metacache.Cache
	flights         singleflightGroup
	exactMu         sync.RWMutex
	exactNotebooks  map[string]WSFileInfo
}

func NewWorkspaceFilesClient(w *databricks.WorkspaceClient) (*WorkspaceFilesClient, error) {
	return NewWorkspaceFilesClientWithConfig(w, CacheConfig{})
}

func NewWorkspaceFilesClientWithConfig(w *databricks.WorkspaceClient, cfg CacheConfig) (*WorkspaceFilesClient, error) {
	databricksClient, err := client.New(w.Config)
	if err != nil {
		return nil, err
	}

	return NewWorkspaceFilesClientWithDepsAndConfig(w.Workspace, databricksClient, nil, cfg), nil
}

func NewWorkspaceFilesClientWithDeps(workspaceClient workspaceClient, apiClient apiDoer, c *metacache.Cache) *WorkspaceFilesClient {
	return NewWorkspaceFilesClientWithDepsAndConfig(workspaceClient, apiClient, c, CacheConfig{})
}

func NewWorkspaceFilesClientWithDepsAndConfig(workspaceClient workspaceClient, apiClient apiDoer, c *metacache.Cache, cfg CacheConfig) *WorkspaceFilesClient {
	if c == nil {
		cfg = cfg.withDefaults()
		c = metacache.NewCacheWithTTLs(cfg.MetadataTTL, cfg.NegativeTTL)
	}
	return &WorkspaceFilesClient{
		workspaceClient: workspaceClient,
		apiClient:       apiClient,
		cache:           c,
		exactNotebooks:  make(map[string]WSFileInfo),
	}
}

func (c *WorkspaceFilesClient) Stat(ctx context.Context, filePath string) (fs.FileInfo, error) {
	info, err := c.statInternal(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	info, err = c.statNotebookBySourceAlias(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	info, err = c.statNotebookByFallbackAlias(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	return nil, fs.ErrNotExist
}

func (c *WorkspaceFilesClient) StatFresh(ctx context.Context, filePath string) (fs.FileInfo, error) {
	info, err := c.statFreshInternal(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	info, err = c.statNotebookBySourceAliasFresh(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	info, err = c.statNotebookByFallbackAliasFresh(ctx, filePath)
	if err == nil {
		return info, nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	return nil, fs.ErrNotExist
}

func (c *WorkspaceFilesClient) statNotebookBySourceAlias(ctx context.Context, filePath string) (fs.FileInfo, error) {
	actualPath, language, ok := pathutil.NotebookRemotePathFromSourcePath(filePath)
	if !ok {
		return nil, fs.ErrNotExist
	}

	info, err := c.statInternal(ctx, actualPath)
	if err != nil {
		return nil, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok || !wsInfo.IsNotebook() || wsInfo.Language != language {
		return nil, fs.ErrNotExist
	}

	return wsInfo, nil
}

func (c *WorkspaceFilesClient) statNotebookByFallbackAlias(ctx context.Context, filePath string) (fs.FileInfo, error) {
	actualPath, ok := pathutil.NotebookRemotePathFromFallbackPath(filePath)
	if !ok {
		return nil, fs.ErrNotExist
	}

	info, err := c.statInternal(ctx, actualPath)
	if err != nil {
		return nil, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok || !wsInfo.IsNotebook() {
		return nil, fs.ErrNotExist
	}

	preferredVisiblePath := pathutil.NotebookVisiblePath(wsInfo.Path, wsInfo.Language)
	if preferredVisiblePath == filePath {
		return wsInfo, nil
	}

	collides, err := c.exactNonNotebookExists(ctx, preferredVisiblePath)
	if err != nil {
		return nil, err
	}
	if collides {
		return wsInfo, nil
	}

	return nil, fs.ErrNotExist
}

func (c *WorkspaceFilesClient) statFreshInternal(ctx context.Context, filePath string) (fs.FileInfo, error) {
	var previousExact WSFileInfo
	if cachedInfo, ok := c.exactNotebookInfoForKey(filePath); ok {
		previousExact = cachedInfo
	}
	c.cache.Invalidate(filePath)
	info, err := c.statFromBackend(ctx, filePath)
	if err != nil {
		return nil, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok {
		return info, nil
	}

	merged, changed := mergeNotebookExactSize(wsInfo, previousExact)
	if changed {
		c.cache.Set(filePath, merged)
		c.setExactNotebookInfo(merged, notebookExactInfoKeys(filePath, merged)...)
	}
	return merged, nil
}

func (c *WorkspaceFilesClient) statNotebookBySourceAliasFresh(ctx context.Context, filePath string) (fs.FileInfo, error) {
	actualPath, language, ok := pathutil.NotebookRemotePathFromSourcePath(filePath)
	if !ok {
		return nil, fs.ErrNotExist
	}

	c.cache.Invalidate(filePath)
	info, err := c.statFreshInternal(ctx, actualPath)
	if err != nil {
		return nil, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok || !wsInfo.IsNotebook() || wsInfo.Language != language {
		return nil, fs.ErrNotExist
	}

	return wsInfo, nil
}

func (c *WorkspaceFilesClient) statNotebookByFallbackAliasFresh(ctx context.Context, filePath string) (fs.FileInfo, error) {
	actualPath, ok := pathutil.NotebookRemotePathFromFallbackPath(filePath)
	if !ok {
		return nil, fs.ErrNotExist
	}

	c.cache.Invalidate(filePath)
	info, err := c.statFreshInternal(ctx, actualPath)
	if err != nil {
		return nil, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok || !wsInfo.IsNotebook() {
		return nil, fs.ErrNotExist
	}

	preferredVisiblePath := pathutil.NotebookVisiblePath(wsInfo.Path, wsInfo.Language)
	if preferredVisiblePath == filePath {
		return wsInfo, nil
	}

	collides, err := c.exactNonNotebookExistsFresh(ctx, preferredVisiblePath)
	if err != nil {
		return nil, err
	}
	if collides {
		return wsInfo, nil
	}

	return nil, fs.ErrNotExist
}

func (c *WorkspaceFilesClient) exactNonNotebookExists(ctx context.Context, filePath string) (bool, error) {
	info, err := c.statInternal(ctx, filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok {
		return true, nil
	}

	return !wsInfo.IsNotebook(), nil
}

func (c *WorkspaceFilesClient) exactNonNotebookExistsFresh(ctx context.Context, filePath string) (bool, error) {
	info, err := c.statFreshInternal(ctx, filePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok {
		return true, nil
	}

	return !wsInfo.IsNotebook(), nil
}

func notebookVisibleName(info WSFileInfo, usedNames map[string]struct{}) (string, bool) {
	preferred := pathutil.NotebookVisibleName(info.Name(), info.Language)
	if _, exists := usedNames[preferred]; !exists {
		usedNames[preferred] = struct{}{}
		return preferred, true
	}

	fallback := pathutil.NotebookFallbackName(info.Name())
	if _, exists := usedNames[fallback]; exists {
		logging.Debugf("ReadDir cache: hiding notebook %s because both %s and %s collide", info.Path, preferred, fallback)
		return "", false
	}

	usedNames[fallback] = struct{}{}
	return fallback, true
}

func sameNotebookIdentity(a, b WSFileInfo) bool {
	if !a.IsNotebook() || !b.IsNotebook() {
		return false
	}
	return a.Path == b.Path &&
		a.ModifiedAt == b.ModifiedAt &&
		a.ObjectId == b.ObjectId &&
		a.ResourceId == b.ResourceId
}

func mergeNotebookExactSize(info WSFileInfo, exact WSFileInfo) (WSFileInfo, bool) {
	if !info.IsNotebook() || !exact.IsNotebook() || !exact.NotebookSizeComputed {
		return info, false
	}
	if !sameNotebookIdentity(info, exact) {
		return info, false
	}
	if info.NotebookSizeComputed && info.Size() == exact.Size() {
		return info, false
	}
	info.ObjectInfo.Size = exact.Size()
	info.NotebookSizeComputed = true
	return info, true
}

func notebookInfoKeys(cacheKey string, info WSFileInfo) []string {
	keys := make([]string, 0, 3)
	seen := make(map[string]struct{}, 3)
	add := func(key string) {
		if key == "" {
			return
		}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	add(info.Path)
	add(cacheKey)
	if info.IsNotebook() && info.Path != "" {
		add(pathutil.NotebookVisiblePath(info.Path, info.Language))
	}

	return keys
}

func notebookExactInfoKeys(cacheKey string, info WSFileInfo) []string {
	if !info.IsNotebook() || !info.NotebookSizeComputed {
		return nil
	}
	return notebookInfoKeys(cacheKey, info)
}

func notebookInvalidateTargets(filePath string) map[string]struct{} {
	targets := map[string]struct{}{
		filePath: {},
	}
	if actualPath, _, ok := pathutil.NotebookRemotePathFromSourcePath(filePath); ok {
		targets[actualPath] = struct{}{}
	}
	if actualPath, ok := pathutil.NotebookRemotePathFromFallbackPath(filePath); ok {
		targets[actualPath] = struct{}{}
	}
	return targets
}

func pathMatchesInvalidation(candidate string, target string) bool {
	if target == "/" {
		return strings.HasPrefix(candidate, "/")
	}
	if candidate == target {
		return true
	}
	return strings.HasPrefix(candidate, strings.TrimSuffix(target, "/")+"/")
}

func (c *WorkspaceFilesClient) setExactNotebookInfo(info WSFileInfo, keys ...string) {
	if !info.IsNotebook() || !info.NotebookSizeComputed {
		return
	}

	c.exactMu.Lock()
	defer c.exactMu.Unlock()
	for _, key := range keys {
		if key == "" {
			continue
		}
		c.exactNotebooks[key] = info
	}
}

func (c *WorkspaceFilesClient) exactNotebookInfoForKey(key string) (WSFileInfo, bool) {
	if key == "" {
		return WSFileInfo{}, false
	}

	c.exactMu.RLock()
	info, ok := c.exactNotebooks[key]
	c.exactMu.RUnlock()
	if ok {
		return info, true
	}

	cached, found := c.cache.Get(key)
	if !found || cached == nil {
		return WSFileInfo{}, false
	}
	cachedInfo, ok := toWSFileInfo(cached)
	if !ok || !cachedInfo.IsNotebook() || !cachedInfo.NotebookSizeComputed {
		return WSFileInfo{}, false
	}
	return cachedInfo, true
}

func (c *WorkspaceFilesClient) deleteExactNotebookInfo(keys ...string) {
	if len(keys) == 0 {
		return
	}

	c.exactMu.Lock()
	defer c.exactMu.Unlock()
	for _, key := range keys {
		delete(c.exactNotebooks, key)
	}
}

func (c *WorkspaceFilesClient) invalidateExactNotebookInfo(filePath string) {
	targets := notebookInvalidateTargets(filePath)

	c.exactMu.Lock()
	defer c.exactMu.Unlock()

	for key, info := range c.exactNotebooks {
		for target := range targets {
			if pathMatchesInvalidation(key, target) || pathMatchesInvalidation(info.Path, target) {
				delete(c.exactNotebooks, key)
				break
			}
		}
	}
}

func (c *WorkspaceFilesClient) cachedExactNotebookInfo(cacheKey string, info WSFileInfo) (WSFileInfo, bool) {
	candidates := notebookInfoKeys(cacheKey, info)

	for _, key := range candidates {
		cachedInfo, found := c.exactNotebookInfoForKey(key)
		if !found {
			continue
		}
		if !sameNotebookIdentity(info, cachedInfo) {
			c.deleteExactNotebookInfo(notebookInfoKeys(key, cachedInfo)...)
			continue
		}
		if merged, ok := mergeNotebookExactSize(info, cachedInfo); ok {
			return merged, true
		}
		return info, false
	}

	return info, false
}

func (c *WorkspaceFilesClient) preserveNotebookExactSize(cacheKey string, info fs.FileInfo) fs.FileInfo {
	wsInfo, ok := toWSFileInfo(info)
	if !ok || !wsInfo.IsNotebook() {
		return info
	}

	merged, changed := c.cachedExactNotebookInfo(cacheKey, wsInfo)
	if changed {
		c.cache.Set(cacheKey, merged)
		c.setExactNotebookInfo(merged, notebookExactInfoKeys(cacheKey, merged)...)
		return merged
	}

	return wsInfo
}

func (c *WorkspaceFilesClient) rememberNotebookExactSize(cacheKey string, info WSFileInfo, size int64) WSFileInfo {
	if !info.IsNotebook() {
		return info
	}

	info.ObjectInfo.Size = size
	info.NotebookSizeComputed = true

	keys := []string{}
	if info.Path != "" {
		keys = append(keys, info.Path)
	}
	if cacheKey != "" && cacheKey != info.Path {
		keys = append(keys, cacheKey)
	}
	for _, key := range keys {
		c.cache.Set(key, info)
	}
	c.setExactNotebookInfo(info, keys...)

	return info
}

func (c *WorkspaceFilesClient) statFromBackend(ctx context.Context, filePath string) (fs.FileInfo, error) {
	value, err := c.flights.Do("stat:"+filePath, func() (any, error) {
		var resp objectInfoResponse
		urlPath := fmt.Sprintf(
			"/api/2.0/workspace-files/object-info?path=%s",
			url.QueryEscape(filePath),
		)

		if err := c.apiClient.Do(ctx, http.MethodGet, urlPath, nil, nil, nil, &resp); err != nil {
			c.cache.Set(filePath, nil)
			return nil, normalizeNotExistError(err)
		}

		apiInfo := WSFileInfo{ObjectInfo: resp.WsfsObjectInfo.ObjectInfo}
		if resp.WsfsObjectInfo.SignedURL != nil {
			apiInfo.SignedURL = resp.WsfsObjectInfo.SignedURL.URL
			apiInfo.SignedURLHeaders = resp.WsfsObjectInfo.SignedURL.Headers
		}
		if merged, changed := c.cachedExactNotebookInfo(filePath, apiInfo); changed {
			apiInfo = merged
		}
		c.cache.Set(filePath, apiInfo)
		return apiInfo, nil
	})
	if err != nil {
		return nil, err
	}

	info, ok := value.(WSFileInfo)
	if !ok {
		return nil, fmt.Errorf("unexpected stat result type %T", value)
	}
	return info, nil
}

func (c *WorkspaceFilesClient) statInternal(ctx context.Context, filePath string) (fs.FileInfo, error) {
	directInfo, directFound := c.cache.Get(filePath)
	if directFound && directInfo != nil {
		return c.preserveNotebookExactSize(filePath, directInfo), nil
	}

	if info, found := c.cache.LookupDirEntry(filePath); found {
		if info == nil {
			c.cache.Set(filePath, nil)
			return nil, fs.ErrNotExist
		}
		return c.preserveNotebookExactSize(filePath, info), nil
	}

	if directFound {
		return nil, fs.ErrNotExist
	}

	return c.statFromBackend(ctx, filePath)
}

func (c *WorkspaceFilesClient) ReadDir(ctx context.Context, dirPath string) ([]fs.DirEntry, error) {
	if entries, found := c.cache.GetDirEntries(dirPath); found {
		return entries, nil
	}

	value, err := c.flights.Do("readdir:"+dirPath, func() (any, error) {
		if entries, found := c.cache.GetDirEntries(dirPath); found {
			return entries, nil
		}

		var resp listFilesResponse
		urlPath := fmt.Sprintf(
			"/api/2.0/workspace-files/list-files?path=%s",
			url.QueryEscape(dirPath),
		)

		if err := c.apiClient.Do(ctx, http.MethodGet, urlPath, nil, nil, nil, &resp); err != nil {
			return nil, normalizeNotExistError(err)
		}

		entries := make([]fs.DirEntry, len(resp.Objects))
		lookup := make([]metacache.DirLookupEntry, 0, len(resp.Objects))
		usedNames := make(map[string]struct{}, len(resp.Objects))
		notebooks := make([]WSFileInfo, 0, len(resp.Objects))

		for i, obj := range resp.Objects {
			info := WSFileInfo{
				ObjectInfo: obj.ObjectInfo,
			}
			if obj.SignedURL != nil {
				info.SignedURL = obj.SignedURL.URL
				info.SignedURLHeaders = obj.SignedURL.Headers
			}
			if merged, changed := c.cachedExactNotebookInfo(info.Path, info); changed {
				info = merged
			}

			entry := WSDirEntry{info}
			entries[i] = entry
			c.cache.Set(info.Path, info)

			if info.IsNotebook() {
				notebooks = append(notebooks, info)
				continue
			}
			name := entry.Name()
			usedNames[name] = struct{}{}
			lookup = append(lookup, metacache.DirLookupEntry{Name: name, Info: info})
		}

		for _, info := range notebooks {
			name, visible := notebookVisibleName(info, usedNames)
			if !visible {
				continue
			}
			lookup = append(lookup, metacache.DirLookupEntry{Name: name, Info: info})
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Name() < entries[j].Name()
		})

		c.cache.SetDirEntries(dirPath, entries, lookup)
		return entries, nil
	})
	if err != nil {
		return nil, err
	}

	entries, ok := value.([]fs.DirEntry)
	if !ok {
		return nil, fmt.Errorf("unexpected readdir result type %T", value)
	}
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

	// Use retryable HTTP client for transient errors (429, 5xx)
	httpClient := retry.NewHTTPClient(httpTimeout, retry.DefaultConfig())
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("signed URL GET failed with status: %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

func (c *WorkspaceFilesClient) exportNotebookSource(ctx context.Context, filepath string) ([]byte, error) {
	resp, err := c.workspaceClient.Export(ctx, workspace.ExportRequest{
		Path:   filepath,
		Format: workspace.ExportFormatSource,
	})
	if err != nil {
		return nil, err
	}
	return base64.StdEncoding.DecodeString(resp.Content)
}

func (c *WorkspaceFilesClient) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	value, err := c.flights.Do("read:"+filePath, func() (any, error) {
		info, err := c.Stat(ctx, filePath)
		if err != nil {
			return nil, err
		}

		wsInfo, ok := toWSFileInfo(info)
		if !ok {
			return nil, fmt.Errorf("unexpected file info type for %s", filePath)
		}

		actualPath := wsInfo.Path
		if actualPath == "" {
			actualPath = filePath
		}

		// For notebooks, use Export with SOURCE format.
		if wsInfo.IsNotebook() {
			logging.Debugf("Read notebook via Export (SOURCE format) for path: %s", actualPath)
			data, err := c.exportNotebookSource(ctx, actualPath)
			if err != nil {
				return nil, err
			}
			c.rememberNotebookExactSize(filePath, wsInfo, int64(len(data)))
			return data, nil
		}

		fileSize := wsInfo.Size()
		if fileSize < sizeThresholdForSignedURL {
			logging.Debugf("Read via Export (size %d < %d threshold) for path: %s", fileSize, sizeThresholdForSignedURL, actualPath)
			return c.exportNotebookSource(ctx, actualPath)
		}

		if wsInfo.SignedURL != "" {
			logging.Debugf("Read via signed URL (size %d >= %d threshold) for path: %s", fileSize, sizeThresholdForSignedURL, actualPath)
			data, err := c.readViaSignedURL(ctx, wsInfo.SignedURL, wsInfo.SignedURLHeaders)
			if err == nil {
				return data, nil
			}
			logging.Debugf("Read via signed URL failed for path: %s, falling back to Export: %s", actualPath, sanitizeError(err))
		}

		return c.exportNotebookSource(ctx, actualPath)
	})
	if err != nil {
		return nil, err
	}

	data, ok := value.([]byte)
	if !ok {
		return nil, fmt.Errorf("unexpected read result type %T", value)
	}
	return data, nil
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

	// 2. Upload to signed URL with PUT (with retry for transient errors)
	signedURL := resp.SignedURLs[0]
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL.URL, bytes.NewReader(data))
	if err != nil {
		return err
	}

	for k, v := range signedURL.Headers {
		req.Header.Set(k, v)
	}

	// Use retryable HTTP client for transient errors (429, 5xx)
	httpClient := retry.NewHTTPClient(httpTimeout, retry.DefaultConfig())
	putResp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer putResp.Body.Close()

	if putResp.StatusCode != http.StatusOK && putResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(putResp.Body)
		return fmt.Errorf("signed URL PUT failed with status %d: %s", putResp.StatusCode, truncateBody(string(body), maxErrorBodyLen))
	}

	return nil
}

func (c *WorkspaceFilesClient) writeViaImportFile(ctx context.Context, filepath string, data []byte) error {
	urlPath := fmt.Sprintf(
		"/api/2.0/workspace-files/import-file/%s?overwrite=true",
		url.PathEscape(strings.TrimLeft(filepath, "/")),
	)
	return c.apiClient.Do(ctx, http.MethodPost, urlPath, nil, nil, data, nil)
}

func detectNotebookLanguageFromSource(data []byte) workspace.Language {
	// Only inspect the first line — no need to copy the entire slice.
	end := bytes.IndexByte(data, '\n')
	if end < 0 {
		end = len(data)
	}
	firstLine := string(bytes.TrimSuffix(data[:end], []byte("\r")))

	switch {
	case strings.HasPrefix(firstLine, "--"):
		return workspace.LanguageSql
	case strings.HasPrefix(firstLine, "//"):
		return workspace.LanguageScala
	case strings.HasPrefix(firstLine, "#"):
		return workspace.LanguagePython
	default:
		return ""
	}
}

func normalizeNotebookLanguage(language workspace.Language, data []byte) workspace.Language {
	if language != "" {
		return language
	}
	if detected := detectNotebookLanguageFromSource(data); detected != "" {
		return detected
	}
	return workspace.LanguagePython
}

func (c *WorkspaceFilesClient) writeRegularFile(ctx context.Context, actualPath string, data []byte) error {
	c.cache.Invalidate(actualPath)

	if len(data) < sizeThresholdForSignedURL {
		logging.Debugf("Write via import-file (size %d < %d threshold) for path: %s", len(data), sizeThresholdForSignedURL, actualPath)
		return c.writeViaImportFile(ctx, actualPath, data)
	}

	logging.Debugf("Write via new-files (size %d >= %d threshold) for path: %s", len(data), sizeThresholdForSignedURL, actualPath)
	err := c.writeViaNewFiles(ctx, actualPath, data)
	if err == nil {
		return nil
	}
	logging.Debugf("Write via new-files failed for path: %s, falling back to import-file: %s", actualPath, sanitizeError(err))

	return c.writeViaImportFile(ctx, actualPath, data)
}

func (c *WorkspaceFilesClient) writeNotebookSource(ctx context.Context, actualPath string, language workspace.Language, data []byte) error {
	c.cache.Invalidate(actualPath)
	return c.workspaceClient.Upload(
		ctx,
		actualPath,
		bytes.NewReader(data),
		workspace.UploadFormat(workspace.ImportFormatSource),
		workspace.UploadLanguage(normalizeNotebookLanguage(language, data)),
		workspace.UploadOverwrite(),
	)
}

func (c *WorkspaceFilesClient) Write(ctx context.Context, filepath string, data []byte) error {
	info, err := c.Stat(ctx, filepath)
	if err == nil {
		wsInfo, ok := toWSFileInfo(info)
		if !ok {
			return fmt.Errorf("unexpected file info type for %s", filepath)
		}
		c.cache.Invalidate(filepath)
		c.cache.Invalidate(wsInfo.Path)

		var writeErr error
		if wsInfo.IsNotebook() {
			logging.Debugf("Writing to notebook: %s", filepath)
			writeErr = c.writeNotebookSource(ctx, wsInfo.Path, wsInfo.Language, data)
		} else {
			writeErr = c.writeRegularFile(ctx, wsInfo.Path, data)
		}
		if writeErr == nil {
			c.cache.Invalidate(filepath)
			c.cache.Invalidate(wsInfo.Path)
		}
		return writeErr
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	if actualPath, language, ok := pathutil.NotebookRemotePathFromSourcePath(filepath); ok {
		c.cache.Invalidate(filepath)
		c.cache.Invalidate(actualPath)
		logging.Debugf("Creating new notebook: %s", filepath)
		writeErr := c.writeNotebookSource(ctx, actualPath, language, data)
		if writeErr == nil {
			c.cache.Invalidate(filepath)
			c.cache.Invalidate(actualPath)
		}
		return writeErr
	}

	c.cache.Invalidate(filepath)
	writeErr := c.writeRegularFile(ctx, filepath, data)
	if writeErr == nil {
		c.cache.Invalidate(filepath)
	}
	return writeErr
}

func (c *WorkspaceFilesClient) Delete(ctx context.Context, filePath string, recursive bool) error {
	actualPath := filePath
	info, err := c.Stat(ctx, filePath)
	if err == nil {
		if wsInfo, ok := toWSFileInfo(info); ok {
			actualPath = wsInfo.Path
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	c.cache.Invalidate(filePath)
	c.cache.Invalidate(actualPath)

	return c.workspaceClient.Delete(ctx, workspace.Delete{
		Path:      actualPath,
		Recursive: recursive,
	})
}

func (c *WorkspaceFilesClient) Mkdir(ctx context.Context, dirPath string) error {
	c.cache.Invalidate(dirPath)

	return c.workspaceClient.Mkdirs(ctx, workspace.Mkdirs{
		Path: dirPath,
	})
}

type notebookRenameTarget struct {
	path     string
	language workspace.Language
}

func resolveNotebookRenameTarget(destinationPath string, currentLanguage workspace.Language) (notebookRenameTarget, error) {
	if actualPath, language, ok := pathutil.NotebookRemotePathFromSourcePath(destinationPath); ok {
		return notebookRenameTarget{path: actualPath, language: language}, nil
	}
	if actualPath, ok := pathutil.NotebookRemotePathFromFallbackPath(destinationPath); ok {
		return notebookRenameTarget{path: actualPath, language: currentLanguage}, nil
	}
	return notebookRenameTarget{}, fmt.Errorf("%w: notebook destination must use a supported extension (%s or %s)",
		fs.ErrInvalid,
		strings.Join(pathutil.AllNotebookSourceSuffixes(), ", "), pathutil.NotebookFallbackSuffix)
}

func replaceIfMatch(line, replacement string, candidates []string) string {
	for _, candidate := range candidates {
		if line == candidate {
			return replacement
		}
	}
	return line
}

func normalizeLineEndings(data []byte) string {
	return strings.ReplaceAll(string(data), "\r\n", "\n")
}

func convertNotebookSourceLanguage(data []byte, language workspace.Language) []byte {
	source := normalizeLineEndings(data)
	hadTrailingNewline := strings.HasSuffix(source, "\n")
	if hadTrailingNewline {
		source = strings.TrimSuffix(source, "\n")
	}
	if source == "" {
		return data
	}

	lines := strings.Split(source, "\n")
	headers := pathutil.AllNotebookSourceHeaders()
	delimiters := pathutil.AllNotebookCellDelimiters()
	for i, line := range lines {
		if i == 0 {
			lines[i] = replaceIfMatch(line, pathutil.NotebookSourceHeader(language), headers)
			continue
		}
		lines[i] = replaceIfMatch(line, pathutil.NotebookCellDelimiter(language), delimiters)
	}

	converted := strings.Join(lines, "\n")
	if hadTrailingNewline {
		converted += "\n"
	}
	return []byte(converted)
}

func (c *WorkspaceFilesClient) renameExactPath(ctx context.Context, actualSource string, actualDest string) error {
	urlPath := "/api/2.0/workspace/rename"

	reqBody := map[string]any{
		"source_path":      actualSource,
		"destination_path": actualDest,
	}

	if err := c.apiClient.Do(ctx, http.MethodPost, urlPath, nil, nil, reqBody, nil); err != nil {
		return err
	}

	c.cache.Invalidate(actualSource)
	c.cache.Invalidate(actualDest)
	return nil
}

func (c *WorkspaceFilesClient) renameNotebook(ctx context.Context, sourceInfo WSFileInfo, destinationPath string) error {
	target, err := resolveNotebookRenameTarget(destinationPath, sourceInfo.Language)
	if err != nil {
		return err
	}

	if target.path == sourceInfo.Path && target.language == sourceInfo.Language {
		return nil
	}

	if target.language == sourceInfo.Language {
		return c.renameExactPath(ctx, sourceInfo.Path, target.path)
	}

	data, err := c.exportNotebookSource(ctx, sourceInfo.Path)
	if err != nil {
		return err
	}

	converted := convertNotebookSourceLanguage(data, target.language)
	if err := c.writeNotebookSource(ctx, target.path, target.language, converted); err != nil {
		return err
	}

	if target.path == sourceInfo.Path {
		c.cache.Invalidate(sourceInfo.Path)
		return nil
	}

	if err := c.workspaceClient.Delete(ctx, workspace.Delete{
		Path:      sourceInfo.Path,
		Recursive: false,
	}); err != nil {
		return err
	}

	c.cache.Invalidate(sourceInfo.Path)
	c.cache.Invalidate(target.path)
	return nil
}

func (c *WorkspaceFilesClient) Rename(ctx context.Context, source_path string, destination_path string) error {
	info, err := c.Stat(ctx, source_path)
	if err != nil {
		return err
	}

	wsInfo, ok := toWSFileInfo(info)
	if !ok {
		return fmt.Errorf("unexpected file info type for %s", source_path)
	}
	c.cache.Invalidate(source_path)
	c.cache.Invalidate(destination_path)
	c.cache.Invalidate(wsInfo.Path)
	if wsInfo.IsNotebook() {
		return c.renameNotebook(ctx, wsInfo, destination_path)
	}
	return c.renameExactPath(ctx, wsInfo.Path, destination_path)
}

// Helpers

func (c *WorkspaceFilesClient) CacheSet(filePath string, info fs.FileInfo) {
	c.cache.Set(filePath, info)
	if wsInfo, ok := toWSFileInfo(info); ok {
		c.setExactNotebookInfo(wsInfo, notebookExactInfoKeys(filePath, wsInfo)...)
	}
}

func (c *WorkspaceFilesClient) CacheInvalidate(filePath string) {
	c.cache.Invalidate(filePath)
	c.invalidateExactNotebookInfo(filePath)
}

func (c *WorkspaceFilesClient) MetadataTTL() time.Duration {
	return c.cache.PositiveTTL()
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
