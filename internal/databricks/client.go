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
}

func NewWorkspaceFilesClient(w *databricks.WorkspaceClient) (*WorkspaceFilesClient, error) {
	databricksClient, err := client.New(w.Config)
	if err != nil {
		return nil, err
	}

	return NewWorkspaceFilesClientWithDeps(w.Workspace, databricksClient, nil), nil
}

func NewWorkspaceFilesClientWithDeps(workspaceClient workspaceClient, apiClient apiDoer, c *metacache.Cache) *WorkspaceFilesClient {
	if c == nil {
		c = metacache.NewCache(60 * time.Second)
	}
	return &WorkspaceFilesClient{
		workspaceClient: workspaceClient,
		apiClient:       apiClient,
		cache:           c,
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

func (c *WorkspaceFilesClient) statInternal(ctx context.Context, filePath string) (fs.FileInfo, error) {
	info, found := c.cache.Get(filePath)
	if found {
		if info == nil {
			return nil, fs.ErrNotExist
		}
		if wsInfo, ok := toWSFileInfo(info); ok && wsInfo.IsNotebook() && !wsInfo.NotebookSizeComputed {
			exportPath := wsInfo.Path
			if exportPath == "" {
				exportPath = filePath
			}
			size, err := c.notebookSize(ctx, exportPath)
			if err == nil {
				wsInfo.ObjectInfo.Size = size
				wsInfo.NotebookSizeComputed = true
				c.cache.Set(filePath, wsInfo)
				return wsInfo, nil
			}
			logging.Debugf("Failed to compute notebook size for %s: %s", filePath, sanitizeError(err))
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
		return nil, normalizeNotExistError(err)
	}

	apiInfo := WSFileInfo{ObjectInfo: resp.WsfsObjectInfo.ObjectInfo}
	if resp.WsfsObjectInfo.SignedURL != nil {
		apiInfo.SignedURL = resp.WsfsObjectInfo.SignedURL.URL
		apiInfo.SignedURLHeaders = resp.WsfsObjectInfo.SignedURL.Headers
	}
	if apiInfo.IsNotebook() {
		exportPath := apiInfo.Path
		if exportPath == "" {
			exportPath = filePath
		}
		size, err := c.notebookSize(ctx, exportPath)
		if err == nil {
			apiInfo.ObjectInfo.Size = size
			apiInfo.NotebookSizeComputed = true
		} else {
			logging.Debugf("Failed to compute notebook size for %s: %s", apiInfo.Path, sanitizeError(err))
		}
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
		return nil, normalizeNotExistError(err)
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

func (c *WorkspaceFilesClient) notebookSize(ctx context.Context, filepath string) (int64, error) {
	data, err := c.exportNotebookSource(ctx, filepath)
	if err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func (c *WorkspaceFilesClient) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
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
		return c.exportNotebookSource(ctx, actualPath)
	}

	// Size-based API selection for regular files
	fileSize := wsInfo.Size()

	if fileSize < sizeThresholdForSignedURL {
		// Small files: use Export directly (1 round trip)
		logging.Debugf("Read via Export (size %d < %d threshold) for path: %s", fileSize, sizeThresholdForSignedURL, actualPath)
		return c.exportNotebookSource(ctx, actualPath)
	}

	// Large files: try signed URL first
	if wsInfo.SignedURL != "" {
		logging.Debugf("Read via signed URL (size %d >= %d threshold) for path: %s", fileSize, sizeThresholdForSignedURL, actualPath)
		data, err := c.readViaSignedURL(ctx, wsInfo.SignedURL, wsInfo.SignedURLHeaders)
		if err == nil {
			return data, nil
		}
		logging.Debugf("Read via signed URL failed for path: %s, falling back to Export: %s", actualPath, sanitizeError(err))
	}

	// Fallback: workspace.Export (for large files when signed URL fails or not available)
	return c.exportNotebookSource(ctx, actualPath)
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
		if wsInfo.IsNotebook() {
			logging.Debugf("Writing to notebook: %s", filepath)
			return c.writeNotebookSource(ctx, wsInfo.Path, wsInfo.Language, data)
		}
		return c.writeRegularFile(ctx, wsInfo.Path, data)
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	if actualPath, language, ok := pathutil.NotebookRemotePathFromSourcePath(filepath); ok {
		logging.Debugf("Creating new notebook: %s", filepath)
		return c.writeNotebookSource(ctx, actualPath, language, data)
	}

	return c.writeRegularFile(ctx, filepath, data)
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
	if wsInfo.IsNotebook() {
		return c.renameNotebook(ctx, wsInfo, destination_path)
	}
	return c.renameExactPath(ctx, wsInfo.Path, destination_path)
}

// Helpers

func (c *WorkspaceFilesClient) CacheSet(filePath string, info fs.FileInfo) {
	c.cache.Set(filePath, info)
}

func (c *WorkspaceFilesClient) CacheInvalidate(filePath string) {
	c.cache.Invalidate(filePath)
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
