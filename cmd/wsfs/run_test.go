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

func (f *fakeWorkspaceFilesClient) StatFresh(ctx context.Context, filePath string) (iofs.FileInfo, error) {
	return f.Stat(ctx, filePath)
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

func (f *fakeWorkspaceFilesClient) MetadataTTL() time.Duration { return time.Second }

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
}

func TestParseArgsOverrides(t *testing.T) {
	cfg, err := parseArgs([]string{
		"wsfs",
		"--debug",
		"--log-level=warn",
		"--allow-other",
		"/mnt/wsfs",
	})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if !cfg.debug || cfg.logLevel != "warn" || !cfg.allowOther {
		t.Fatalf("unexpected flags: %+v", cfg)
	}
}

func TestParseArgsMissingMountpoint(t *testing.T) {
	_, err := parseArgs([]string{"wsfs"})
	if err == nil {
		t.Fatal("expected error for missing mount point")
	}
}

func TestValidateConfig(t *testing.T) {
	cfg := cliConfig{}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
	cfg := buildNodeConfig(42, 24, true)
	if cfg.OwnerUid != 42 || cfg.OwnerGid != 24 || cfg.RestrictAccess || cfg.AttrTTL != defaultAttrTTL || cfg.EntryTTL != defaultEntryTTL {
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
	if opts.AttrTimeout == nil || *opts.AttrTimeout != defaultAttrTTL {
		t.Fatalf("unexpected attr timeout: %v", opts.AttrTimeout)
	}
	if opts.EntryTimeout == nil || *opts.EntryTimeout != defaultEntryTTL {
		t.Fatalf("unexpected entry timeout: %v", opts.EntryTimeout)
	}
	if opts.NegativeTimeout == nil || *opts.NegativeTimeout != defaultNegativeTTL {
		t.Fatalf("unexpected negative timeout: %v", opts.NegativeTimeout)
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
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newDiskCache = func() (*filecache.DiskCache, error) {
		return filecache.NewDisabledCache(), nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		if config == nil {
			t.Fatal("expected node config")
		}
		if config.OwnerUid != 123 || config.OwnerGid != 456 || !config.RestrictAccess {
			t.Fatalf("unexpected node config: %+v", config)
		}
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
		return &user.User{Uid: "not-a-number", Gid: "456"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "Failed to parse UID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunParseGIDError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123", Gid: "not-a-number"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err == nil {
		t.Fatal("expected error")
	} else if !strings.Contains(err.Error(), "Failed to parse GID") {
		t.Fatalf("unexpected error: %v", err)
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
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var gotAllowOther bool
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		if config == nil {
			t.Fatal("expected node config")
		}
		if config.OwnerUid != 123 || config.OwnerGid != 456 || config.RestrictAccess {
			t.Fatalf("unexpected allow-other node config: %+v", config)
		}
		return &wsfsfuse.WSNode{}, nil
	}
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

func TestRunUsesCacheEnabledError(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newDiskCache = func() (*filecache.DiskCache, error) {
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
		return &user.User{Uid: "123", Gid: "456"}, nil
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
		return &user.User{Uid: "123", Gid: "456"}, nil
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
		return &user.User{Uid: "123", Gid: "456"}, nil
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
		return &user.User{Uid: "123", Gid: "456"}, nil
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

func TestValidateConfigNoop(t *testing.T) {
	cfg := cliConfig{}
	if err := validateConfig(cfg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunUsesDefaultDiskCacheFactory(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var called bool
	deps.newDiskCache = func() (*filecache.DiskCache, error) {
		called = true
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

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if !called {
		t.Fatal("expected disk cache factory to be called")
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
		return &user.User{Uid: strconv.FormatInt(int64(^uint64(0)>>1), 10), Gid: "456"}, nil
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

func TestParseArgsRemotePath(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "--remote-path=/Users/alice", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if cfg.remotePath != "/Users/alice" {
		t.Fatalf("remotePath = %q, want /Users/alice", cfg.remotePath)
	}
}

func TestParseArgsRemotePathDefault(t *testing.T) {
	cfg, err := parseArgs([]string{"wsfs", "/mnt/wsfs"})
	if err != nil {
		t.Fatalf("parseArgs failed: %v", err)
	}
	if cfg.remotePath != "" {
		t.Fatalf("remotePath should default to empty, got %q", cfg.remotePath)
	}
}

func TestRunPassesRemotePathToRootNode(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newDiskCache = func() (*filecache.DiskCache, error) {
		return filecache.NewDisabledCache(), nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var gotRootPath string
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		gotRootPath = rootPath
		return &wsfsfuse.WSNode{}, nil
	}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return &fakeServer{waitCh: make(chan struct{})}, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}

	if err := run([]string{"wsfs", "--remote-path=/Users/alice", "/mnt/wsfs"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if gotRootPath != "/Users/alice" {
		t.Fatalf("rootPath = %q, want /Users/alice", gotRootPath)
	}
}

func TestRunDefaultsRemotePathToSlash(t *testing.T) {
	deps := defaultDeps()
	deps.initWorkspace = func() (*databrickssdk.WorkspaceClient, error) {
		return &databrickssdk.WorkspaceClient{}, nil
	}
	deps.workspaceMe = func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
		return "Tester", nil
	}
	deps.currentUser = func() (*user.User, error) {
		return &user.User{Uid: "123", Gid: "456"}, nil
	}
	deps.newDiskCache = func() (*filecache.DiskCache, error) {
		return filecache.NewDisabledCache(), nil
	}
	deps.newWorkspaceFilesClient = func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
		return &fakeWorkspaceFilesClient{}, nil
	}

	var gotRootPath string
	deps.newRootNode = func(api databricks.WorkspaceFilesAPI, cache *filecache.DiskCache, rootPath string, registry *wsfsfuse.DirtyNodeRegistry, config *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error) {
		gotRootPath = rootPath
		return &wsfsfuse.WSNode{}, nil
	}
	deps.mount = func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
		return &fakeServer{waitCh: make(chan struct{})}, nil
	}
	deps.signalContext = func() (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx, func() {}
	}

	if err := run([]string{"wsfs", "/mnt/wsfs"}, deps); err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if gotRootPath != "/" {
		t.Fatalf("rootPath = %q, want /", gotRootPath)
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
		return &user.User{Uid: "123", Gid: "456"}, nil
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
