package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os/user"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	databrickssdk "github.com/databricks/databricks-sdk-go"

	"github.com/hanwen/go-fuse/v2/fs"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	wsfsfuse "wsfs/internal/fuse"
)

type fakeServer struct {
	waitCh    chan struct{}
	unmountMu sync.Mutex
	unmounted bool
}

func (s *fakeServer) Wait() {
	<-s.waitCh
}

func (s *fakeServer) Unmount() error {
	s.unmountMu.Lock()
	if s.unmounted {
		s.unmountMu.Unlock()
		return nil
	}
	s.unmounted = true
	s.unmountMu.Unlock()
	close(s.waitCh)
	return nil
}

type fakeWorkspaceClient struct{}

type fakeWorkspaceFilesClient struct {
	statFunc func(context.Context, string) (iofs.FileInfo, error)
}

func (f *fakeWorkspaceFilesClient) Stat(ctx context.Context, filePath string) (iofs.FileInfo, error) {
	if f.statFunc != nil {
		return f.statFunc(ctx, filePath)
	}
	return databricks.NewTestFileInfo(filePath, 0, true), nil
}

func (f *fakeWorkspaceFilesClient) ReadDir(ctx context.Context, dirPath string) ([]iofs.DirEntry, error) {
	return nil, nil
}

func (f *fakeWorkspaceFilesClient) ReadAll(ctx context.Context, filePath string) ([]byte, error) {
	return nil, nil
}

func (f *fakeWorkspaceFilesClient) Write(ctx context.Context, filepath string, data []byte) error {
	return nil
}

func (f *fakeWorkspaceFilesClient) Delete(ctx context.Context, filePath string, recursive bool) error {
	return nil
}

func (f *fakeWorkspaceFilesClient) Mkdir(ctx context.Context, dirPath string) error {
	return nil
}

func (f *fakeWorkspaceFilesClient) Rename(ctx context.Context, sourcePath string, destinationPath string) error {
	return nil
}

func (f *fakeWorkspaceFilesClient) CacheSet(path string, info iofs.FileInfo) {}

func (f *fakeWorkspaceFilesClient) CacheInvalidate(filePath string) {}

func TestParseArgsDefaultsAndMountpoint(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if cfg.mountPoint != "/mnt/wsfs" {
		t.Fatalf("mountPoint = %q", cfg.mountPoint)
	}
	if cfg.logLevel != "info" {
		t.Fatalf("logLevel = %q", cfg.logLevel)
	}
	if !cfg.enableCache {
		t.Fatal("enableCache should default to true")
	}
}

func TestParseArgsOverrides(t *testing.T) {
	cfg, err := parseArgs([]string{
		"wsfs",
		"--debug",
		"--log-level=warn",
		"--allow-other",
		"--cache=false",
		"--cache-dir=/tmp/cache",
		"--cache-size=12",
		"--cache-ttl=30m",
		"/mnt/wsfs",
	})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if !cfg.debug || cfg.logLevel != "warn" || !cfg.allowOther || cfg.enableCache {
		t.Fatalf("unexpected flags: %+v", cfg)
	}
	if cfg.cacheDir != "/tmp/cache" || cfg.cacheSizeGB != 12 || cfg.cacheTTL != 30*time.Minute {
		t.Fatalf("unexpected cache config: %+v", cfg)
	}
}

func TestParseArgsMissingMountpoint(t *testing.T) {
	_, err := parseArgs([]string{"wsfs"})
	if err == nil {
		t.Fatal("expected error for missing mount point")
	}
}

func TestValidateConfig(t *testing.T) {
	cfg := cliConfig{enableCache: true, cacheSizeGB: 10, cacheTTL: time.Hour}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg.cacheSizeGB = 0
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected size error")
	}

	cfg.cacheSizeGB = 1001
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected max size error")
	}

	cfg.cacheSizeGB = 10
	cfg.cacheTTL = 0
	if err := validateConfig(cfg); err == nil {
		t.Fatal("expected ttl error")
	}

	cfg.enableCache = false
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("unexpected error with cache disabled: %v", err)
	}
}

func TestVersionString(t *testing.T) {
	oldVersion, oldCommit, oldDate := version, commit, date
	defer func() {
		version, commit, date = oldVersion, oldCommit, oldDate
	}()

	version = "v1"
	commit = "abc"
	date = "2025-01-01"

	got := versionString()
	if !strings.Contains(got, "wsfs v1") || !strings.Contains(got, "commit: abc") || !strings.Contains(got, "built: 2025-01-01") {
		t.Fatalf("unexpected version string: %q", got)
	}
}

func TestBuildNodeConfig(t *testing.T) {
	cfg := buildNodeConfig(42, true)
	if cfg.OwnerUid != 42 || cfg.RestrictAccess {
		t.Fatalf("unexpected node config: %+v", cfg)
	}
}

func TestBuildMountOptions(t *testing.T) {
	opts := buildMountOptions(true, true)
	if !opts.MountOptions.AllowOther {
		t.Fatal("AllowOther should be true")
	}
	if !opts.Debug {
		t.Fatal("Debug should be true")
	}
	if opts.MountOptions.Name != "wsfs" || opts.MountOptions.FsName != "wsfs" {
		t.Fatalf("unexpected mount options: %+v", opts.MountOptions)
	}
}

func TestRunShowVersion(t *testing.T) {
	var out bytes.Buffer
	deps := defaultDeps()
	deps.versionOut = func(s string) { _, _ = io.Copy(&out, strings.NewReader(s)) }

	if err := run([]string{"wsfs", "--version"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !strings.Contains(out.String(), "wsfs") {
		t.Fatalf("expected version output, got %q", out.String())
	}
}

func TestRunInvalidConfig(t *testing.T) {
	deps := defaultDeps()
	args := []string{"wsfs", "--cache-size=0", "/mnt/wsfs"}
	if err := run(args, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunInitWorkspaceError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return nil, errors.New("boom")
	}

	args := []string{"wsfs", "/mnt/wsfs"}
	if err := run(args, deps); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "Failed to create Databricks client") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunSuccess(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newDiskCache = func(path string, size int64, ttl time.Duration) (*filecache.DiskCache, error) {
		return filecache.NewDisabledCache(), nil
	}
	deps.newDisabledCache = func() *filecache.DiskCache { return filecache.NewDisabledCache() }
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		return &wsfsfuse.WSNode{}, nil
	}
	server := &fakeServer{waitCh: make(chan struct{})}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return server, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		return ctx, cancel
	}

	done := make(chan error, 1)
	go func() {
		done <- run([]string{"wsfs", "/mnt/wsfs"}, deps)
	}()

	// Allow goroutine to start.
	time.Sleep(10 * time.Millisecond)
	if err := server.Unmount(); err != nil {
		t.Fatalf("unmount failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRunParseUIDError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "not-a-number"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunMountOptionsUsesAllowOther(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var gotAllowOther bool
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		gotAllowOther = opts.MountOptions.AllowOther
		return &fakeServer{waitCh: make(chan struct{})}, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}

	args := []string{"wsfs", "--allow-other", "/mnt/wsfs"}
	if err := run(args, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !gotAllowOther {
		t.Fatal("expected allow-other mount option")
	}
}

func TestRunUsesCacheDisabled(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.newDisabledCache = func() *filecache.DiskCache { return filecache.NewDisabledCache() }
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return &fakeServer{waitCh: make(chan struct{})}, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}

	if err := run([]string{"wsfs", "--cache=false", "/mnt/wsfs"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
}

func TestRunUsesCacheEnabledError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newDiskCache = func(path string, size int64, ttl time.Duration) (*filecache.DiskCache, error) {
		return nil, fmt.Errorf("cache error")
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunNewRootNodeError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		return nil, fmt.Errorf("root error")
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunMountError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return nil, errors.New("mount error")
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestParseArgsFlagError(t *testing.T) {
	_, err := parseArgs([]string{"wsfs", "--unknown"})
	if err == nil {
		t.Fatal("expected error")
	}
	var cliErr *cliError
	if !errors.As(err, &cliErr) {
		t.Fatalf("expected cliError, got %T", err)
	}
	if cliErr.exitCode != 2 || !cliErr.printed {
		t.Fatalf("unexpected cliError: %+v", cliErr)
	}
}

func TestParseArgsEmptyArgs(t *testing.T) {
	_, err := parseArgs([]string{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRunWorkspaceMeError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "", errors.New("me error")
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunCurrentUserError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return nil, errors.New("user error")
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunNewWorkspaceFilesClientError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return nil, errors.New("client error")
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunSignalFlushErrors(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		return &wsfsfuse.WSNode{}, nil
	}
	server := &fakeServer{waitCh: make(chan struct{})}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return server, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		return ctx, cancel
	}

	done := make(chan error, 1)
	go func() {
		done <- run([]string{"wsfs", "/mnt/wsfs"}, deps)
	}()

	cancel()
	_ = server.Unmount()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestParseArgsUsageMessage(t *testing.T) {
	_, err := parseArgs([]string{"wsfs", "--help"})
	if err == nil {
		t.Fatal("expected error")
	}
	var cliErr *cliError
	if !errors.As(err, &cliErr) {
		t.Fatalf("expected cliError, got %T", err)
	}
	if cliErr.exitCode != 0 || !cliErr.printed {
		t.Fatalf("unexpected cliError: %+v", cliErr)
	}
}

func TestValidateConfigCacheDisabled(t *testing.T) {
	cfg := cliConfig{enableCache: false, cacheSizeGB: -1, cacheTTL: -1}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunCacheSizeCalculation(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var gotSize int64
	deps.newDiskCache = func(path string, size int64, ttl time.Duration) (*filecache.DiskCache, error) {
		gotSize = size
		return filecache.NewDisabledCache(), nil
	}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return &fakeServer{waitCh: make(chan struct{})}, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}

	if err := run([]string{"wsfs", "--cache-size=1.5", "/mnt/wsfs"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	expected := int64(1.5 * 1024 * 1024 * 1024)
	if gotSize != expected {
		t.Fatalf("cache size bytes = %d, want %d", gotSize, expected)
	}
}

func TestParseArgsShowVersionNoMountPoint(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "--version"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if !cfg.showVersion {
		t.Fatal("expected showVersion true")
	}
}

func TestRunParseArgsErrorExitCode(t *testing.T) {
	deps := defaultDeps()
	_, err := parseArgs([]string{})
	if err == nil {
		t.Fatal("expected error")
	}
	var cliErr *cliError
	if !errors.As(err, &cliErr) {
		t.Fatal("expected cliError")
	}
	if cliErr.exitCode != 1 {
		t.Fatalf("exitCode = %d", cliErr.exitCode)
	}
	if cliErr.msg == "" {
		t.Fatal("expected message")
	}
	_ = deps
}

func TestRunInvalidUIDType(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: strconv.FormatInt(int64(^uint64(0)>>1), 10)}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err != nil {
		if !strings.Contains(err.Error(), "Failed to parse UID") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestParseArgsMountPointIgnoredForVersion(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "--version", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if !cfg.showVersion {
		t.Fatal("expected showVersion true")
	}
}

func TestParseArgsInvalidDuration(t *testing.T) {
	_, err := parseArgs([]string{"wsfs", "--cache-ttl=invalid", "/mnt/wsfs"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRunMountPointRequired(t *testing.T) {
	deps := defaultDeps()
	if err := run([]string{"wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	}
}

func TestRunShowVersionIgnoresMountPointValidation(t *testing.T) {
	deps := defaultDeps()
	var out bytes.Buffer
	deps.versionOut = func(s string) { out.WriteString(s) }

	if err := run([]string{"wsfs", "--version"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !strings.Contains(out.String(), "wsfs") {
		t.Fatalf("unexpected output: %q", out.String())
	}
}

func TestParseArgsAllowOtherDefaultFalse(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if cfg.allowOther {
		t.Fatal("allowOther should default to false")
	}
}

func TestParseArgsCacheDefaults(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if !cfg.enableCache {
		t.Fatal("enableCache should default to true")
	}
	if cfg.cacheSizeGB <= 0 {
		t.Fatal("cacheSizeGB should be > 0")
	}
	if cfg.cacheTTL <= 0 {
		t.Fatal("cacheTTL should be > 0")
	}
}

func TestParseArgsCacheSizeFloat(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "--cache-size=1.25", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if cfg.cacheSizeGB != 1.25 {
		t.Fatalf("cacheSizeGB = %v", cfg.cacheSizeGB)
	}
}

func TestRunSignalContextCancel(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	server := &fakeServer{waitCh: make(chan struct{})}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return server, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		return ctx, cancel
	}

	done := make(chan error, 1)
	go func() { done <- run([]string{"wsfs", "/mnt/wsfs"}, deps) }()
	server.Unmount()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return")
	}
}
