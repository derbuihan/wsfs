package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	databrickssdk "github.com/databricks/databricks-sdk-go"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	wsfsfuse "wsfs/internal/fuse"
	"wsfs/internal/logging"
)

// Shutdown timeout for flushing dirty buffers
const shutdownTimeout = 30 * time.Second

// cliConfig captures parsed command-line flags.
type cliConfig struct {
	showVersion bool
	debug       bool
	logLevel    string
	allowOther  bool
	enableCache bool
	cacheDir    string
	cacheSizeGB float64
	cacheTTL    time.Duration
	mountPoint  string
}

type cliError struct {
	exitCode int
	msg      string
	printed  bool
}

func (e *cliError) Error() string {
	return e.msg
}

type mountServer interface {
	Wait()
	Unmount() error
}

type runDeps struct {
	initWorkspace           func() (*databrickssdk.WorkspaceClient, error)
	workspaceMe             func(context.Context, *databrickssdk.WorkspaceClient) (string, error)
	currentUser             func() (*user.User, error)
	newDiskCache            func(string, int64, time.Duration) (*filecache.DiskCache, error)
	newDisabledCache        func() *filecache.DiskCache
	newWorkspaceFilesClient func(*databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error)
	newRootNode             func(databricks.WorkspaceFilesAPI, *filecache.DiskCache, string, *wsfsfuse.DirtyNodeRegistry, *wsfsfuse.NodeConfig) (*wsfsfuse.WSNode, error)
	mount                   func(string, fs.InodeEmbedder, *fs.Options) (mountServer, error)
	signalContext           func() (context.Context, context.CancelFunc)
	versionOut              func(string)
}

func defaultDeps() runDeps {
	return runDeps{
		initWorkspace: func() (*databrickssdk.WorkspaceClient, error) {
			return databrickssdk.NewWorkspaceClient()
		},
		workspaceMe: func(ctx context.Context, w *databrickssdk.WorkspaceClient) (string, error) {
			me, err := w.CurrentUser.Me(ctx)
			if err != nil {
				return "", err
			}
			return me.DisplayName, nil
		},
		currentUser:      user.Current,
		newDiskCache:     filecache.NewDiskCache,
		newDisabledCache: filecache.NewDisabledCache,
		newWorkspaceFilesClient: func(w *databrickssdk.WorkspaceClient) (databricks.WorkspaceFilesAPI, error) {
			return databricks.NewWorkspaceFilesClient(w)
		},
		newRootNode: wsfsfuse.NewRootNode,
		mount: func(mountPoint string, root fs.InodeEmbedder, opts *fs.Options) (mountServer, error) {
			return fs.Mount(mountPoint, root, opts)
		},
		signalContext: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		},
		versionOut: func(s string) {
			fmt.Print(s)
		},
	}
}

func parseArgs(args []string) (cliConfig, error) {
	var cfg cliConfig
	if len(args) == 0 {
		return cfg, &cliError{exitCode: 1, msg: "Usage: wsfs MOUNTPOINT"}
	}

	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)

	showVersion := fs.Bool("version", false, "print version and exit")
	debug := fs.Bool("debug", false, "print debug data (equivalent to --log-level=debug)")
	logLevel := fs.String("log-level", "info", "log level: debug, info, warn, error")
	allowOther := fs.Bool("allow-other", false, "allow other users to access the mount")
	enableCache := fs.Bool("cache", true, "enable disk cache for file contents")
	cacheDir := fs.String("cache-dir", filepath.Join(os.TempDir(), "wsfs-cache"), "cache directory path")
	cacheSizeGB := fs.Float64("cache-size", 10, "maximum cache size in GB")
	cacheTTL := fs.Duration("cache-ttl", 24*time.Hour, "cache TTL (e.g., 24h, 30m)")

	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return cfg, &cliError{exitCode: 0, printed: true}
		}
		return cfg, &cliError{exitCode: 2, msg: err.Error(), printed: true}
	}

	cfg = cliConfig{
		showVersion: *showVersion,
		debug:       *debug,
		logLevel:    *logLevel,
		allowOther:  *allowOther,
		enableCache: *enableCache,
		cacheDir:    *cacheDir,
		cacheSizeGB: *cacheSizeGB,
		cacheTTL:    *cacheTTL,
	}

	if fs.NArg() > 0 {
		cfg.mountPoint = fs.Arg(0)
	}

	if cfg.mountPoint == "" && !cfg.showVersion {
		return cfg, &cliError{exitCode: 1, msg: fmt.Sprintf("Usage: %s MOUNTPOINT", args[0])}
	}

	return cfg, nil
}

func validateConfig(cfg cliConfig) error {
	if !cfg.enableCache {
		return nil
	}
	if cfg.cacheSizeGB <= 0 {
		return &cliError{exitCode: 1, msg: fmt.Sprintf("Invalid cache size: %.2f GB (must be positive)", cfg.cacheSizeGB)}
	}
	if cfg.cacheSizeGB > 1000 {
		return &cliError{exitCode: 1, msg: fmt.Sprintf("Invalid cache size: %.2f GB (maximum is 1000 GB)", cfg.cacheSizeGB)}
	}
	if cfg.cacheTTL <= 0 {
		return &cliError{exitCode: 1, msg: fmt.Sprintf("Invalid cache TTL: %v (must be positive)", cfg.cacheTTL)}
	}
	return nil
}

func buildNodeConfig(ownerUid uint32, allowOther bool) *wsfsfuse.NodeConfig {
	return &wsfsfuse.NodeConfig{
		OwnerUid:       ownerUid,
		RestrictAccess: !allowOther,
	}
}

func buildMountOptions(allowOther bool, debug bool) *fs.Options {
	attrTimeout := 30 * time.Second
	entryTimeout := 30 * time.Second
	negativeTimeout := 10 * time.Second

	opts := &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther: allowOther,
			Name:       "wsfs",
			FsName:     "wsfs",
		},
	}
	opts.Debug = debug
	return opts
}

func versionString() string {
	return fmt.Sprintf("wsfs %s (commit: %s, built: %s)\n", version, commit, date)
}

func run(args []string, deps runDeps) error {
	cfg, err := parseArgs(args)
	if err != nil {
		return err
	}

	if cfg.showVersion {
		deps.versionOut(versionString())
		return nil
	}

	// Set log level (--debug takes precedence for backward compatibility)
	if cfg.debug {
		logging.SetLevel(logging.LevelDebug)
	} else {
		logging.SetLevel(logging.ParseLevel(cfg.logLevel))
	}

	if err := validateConfig(cfg); err != nil {
		return err
	}

	// Set up Databricks client
	w, err := deps.initWorkspace()
	if err != nil {
		return fmt.Errorf("Failed to create Databricks client: %w", err)
	}

	displayName, err := deps.workspaceMe(context.Background(), w)
	if err != nil {
		return fmt.Errorf("Failed to get current user: %w", err)
	}
	logging.Infof("Hello, %s! Mounting your Databricks workspace...", displayName)

	// Set up disk cache
	var diskCache *filecache.DiskCache
	if cfg.enableCache {
		cacheSizeBytes := int64(cfg.cacheSizeGB * 1024 * 1024 * 1024)
		diskCache, err = deps.newDiskCache(cfg.cacheDir, cacheSizeBytes, cfg.cacheTTL)
		if err != nil {
			return fmt.Errorf("Failed to create disk cache: %w", err)
		}
		logging.Debugf("Disk cache enabled: dir=%s, size=%.1fGB, ttl=%v", cfg.cacheDir, cfg.cacheSizeGB, cfg.cacheTTL)
	} else {
		diskCache = deps.newDisabledCache()
		logging.Debugf("Disk cache disabled")
	}

	// Set up Databricks FS client
	wfclient, err := deps.newWorkspaceFilesClient(w)
	if err != nil {
		return fmt.Errorf("Failed to create Databricks Workspace Files Client: %w", err)
	}

	// Create dirty node registry for graceful shutdown
	registry := wsfsfuse.NewDirtyNodeRegistry()

	// Get current user's UID for access control
	currentUser, err := deps.currentUser()
	if err != nil {
		return fmt.Errorf("Failed to get current user: %w", err)
	}
	ownerUid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		return fmt.Errorf("Failed to parse UID: %w", err)
	}

	// Create node config for access control
	// When --allow-other is enabled, restrict access to mount owner only
	nodeConfig := buildNodeConfig(uint32(ownerUid), cfg.allowOther)
	if cfg.allowOther {
		logging.Infof("allow-other enabled: all local users can access the mount")
	} else {
		logging.Debugf("Access control enabled: only UID %d can access the mount", ownerUid)
	}

	// Set up Root node
	root, err := deps.newRootNode(wfclient, diskCache, "/", registry, nodeConfig)
	if err != nil {
		return fmt.Errorf("Failed to create root node: %w", err)
	}

	// Mount filesystem
	opts := buildMountOptions(cfg.allowOther, cfg.debug)
	server, err := deps.mount(cfg.mountPoint, root, opts)
	if err != nil {
		return fmt.Errorf("Mount fail: %w", err)
	}
	logging.Infof("Mounted Databricks workspace on %s", cfg.mountPoint)
	logging.Infof("Press Ctrl+C to unmount")

	// Signal handling for graceful shutdown
	ctx, stop := deps.signalContext()
	defer stop()

	var unmountOnce sync.Once
	unmount := func() {
		unmountOnce.Do(func() {
			if err := server.Unmount(); err != nil {
				log.Printf("Unmount error: %v", err)
			}
		})
	}

	// Wait for signal in goroutine
	go func() {
		<-ctx.Done()
		log.Println("Shutdown signal received, flushing dirty buffers...")

		// Flush all dirty buffers with timeout
		flushCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		flushed, errors := registry.FlushAll(flushCtx)
		if len(errors) > 0 {
			for _, err := range errors {
				log.Printf("Flush error: %v", err)
			}
		}
		if flushed > 0 {
			log.Printf("Flushed %d dirty buffer(s)", flushed)
		}

		// Unmount filesystem
		unmount()
	}()

	server.Wait()
	return nil
}
