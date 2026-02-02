package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
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

func main() {
	debug := flag.Bool("debug", false, "print debug data (equivalent to --log-level=debug)")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	allowOther := flag.Bool("allow-other", false, "allow other users to access the mount")

	// Cache configuration
	enableCache := flag.Bool("cache", true, "enable disk cache for file contents")
	cacheDir := flag.String("cache-dir", filepath.Join(os.TempDir(), "wsfs-cache"), "cache directory path")
	cacheSizeGB := flag.Float64("cache-size", 10, "maximum cache size in GB")
	cacheTTL := flag.Duration("cache-ttl", 24*time.Hour, "cache TTL (e.g., 24h, 30m)")

	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatalf("Usage: %s MOUNTPOINT", os.Args[0])
	}

	// Set log level (--debug takes precedence for backward compatibility)
	if *debug {
		logging.SetLevel(logging.LevelDebug)
	} else {
		logging.SetLevel(logging.ParseLevel(*logLevel))
	}

	// Set up Databricks client
	w, err := databrickssdk.NewWorkspaceClient()
	if err != nil {
		log.Fatalf("Failed to create Databricks client: %v", err)
	}
	me, err := w.CurrentUser.Me(context.Background())
	if err != nil {
		log.Fatalf("Failed to get current user: %v", err)
	}
	logging.Infof("Hello, %s! Mounting your Databricks workspace...", me.DisplayName)

	// Set up disk cache
	var diskCache *filecache.DiskCache
	if *enableCache {
		// Validate cache configuration
		if *cacheSizeGB <= 0 {
			log.Fatalf("Invalid cache size: %.2f GB (must be positive)", *cacheSizeGB)
		}
		if *cacheSizeGB > 1000 {
			log.Fatalf("Invalid cache size: %.2f GB (maximum is 1000 GB)", *cacheSizeGB)
		}
		if *cacheTTL <= 0 {
			log.Fatalf("Invalid cache TTL: %v (must be positive)", *cacheTTL)
		}
		cacheSizeBytes := int64(*cacheSizeGB * 1024 * 1024 * 1024)
		diskCache, err = filecache.NewDiskCache(*cacheDir, cacheSizeBytes, *cacheTTL)
		if err != nil {
			log.Fatalf("Failed to create disk cache: %v", err)
		}
		logging.Debugf("Disk cache enabled: dir=%s, size=%.1fGB, ttl=%v", *cacheDir, *cacheSizeGB, *cacheTTL)
	} else {
		diskCache = filecache.NewDisabledCache()
		logging.Debugf("Disk cache disabled")
	}

	// Set up Databricks FS client
	wfclient, err := databricks.NewWorkspaceFilesClient(w)
	if err != nil {
		log.Fatalf("Failed to create Databricks Workspace Files Client: %v", err)
	}

	// Create dirty node registry for graceful shutdown
	registry := wsfsfuse.NewDirtyNodeRegistry()

	// Get current user's UID for access control
	currentUser, err := user.Current()
	if err != nil {
		log.Fatalf("Failed to get current user: %v", err)
	}
	ownerUid, err := strconv.ParseUint(currentUser.Uid, 10, 32)
	if err != nil {
		log.Fatalf("Failed to parse UID: %v", err)
	}

	// Create node config for access control
	// When --allow-other is enabled, restrict access to mount owner only
	nodeConfig := &wsfsfuse.NodeConfig{
		OwnerUid:       uint32(ownerUid),
		RestrictAccess: *allowOther,
	}
	if *allowOther {
		logging.Infof("Access control enabled: only UID %d can access the mount", ownerUid)
	}

	// Set up Root node
	root, err := wsfsfuse.NewRootNode(wfclient, diskCache, "/", registry, nodeConfig)
	if err != nil {
		log.Fatalf("Failed to create root node: %v", err)
	}

	// Mount filesystem
	attrTimeout := 30 * time.Second
	entryTimeout := 30 * time.Second
	negativeTimeout := 10 * time.Second

	opts := &fs.Options{
		AttrTimeout:     &attrTimeout,
		EntryTimeout:    &entryTimeout,
		NegativeTimeout: &negativeTimeout,
		MountOptions: fuse.MountOptions{
			AllowOther: *allowOther,
			Name:       "wsfs",
			FsName:     "wsfs",
		},
	}
	opts.Debug = *debug

	server, err := fs.Mount(flag.Arg(0), root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	logging.Infof("Mounted Databricks workspace on %s", flag.Arg(0))
	logging.Infof("Press Ctrl+C to unmount")

	// Signal handling for graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
		if err := server.Unmount(); err != nil {
			log.Printf("Unmount error: %v", err)
		}
	}()

	server.Wait()
}
