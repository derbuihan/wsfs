package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"time"

	databrickssdk "github.com/databricks/databricks-sdk-go"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"wsfs/internal/databricks"
	"wsfs/internal/filecache"
	wsfsfuse "wsfs/internal/fuse"
	"wsfs/internal/logging"
)

func main() {
	debug := flag.Bool("debug", false, "print debug data")

	// Cache configuration
	enableCache := flag.Bool("cache", true, "enable disk cache for file contents")
	cacheDir := flag.String("cache-dir", filepath.Join(os.TempDir(), "wsfs-cache"), "cache directory path")
	cacheSizeGB := flag.Float64("cache-size", 10, "maximum cache size in GB")
	cacheTTL := flag.Duration("cache-ttl", 24*time.Hour, "cache TTL (e.g., 24h, 30m)")

	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatalf("Usage: %s MOUNTPOINT", os.Args[0])
	}
	logging.DebugLogs = *debug

	// Set up Databricks client
	w, err := databrickssdk.NewWorkspaceClient()
	if err != nil {
		log.Fatalf("Failed to create Databricks client: %v", err)
	}
	me, err := w.CurrentUser.Me(context.Background())
	if err != nil {
		log.Fatalf("Failed to get current user: %v", err)
	}
	logging.Debugf("Hello, %s! Mounting your Databricks workspace...", me.DisplayName)

	// Set up disk cache
	var diskCache *filecache.DiskCache
	if *enableCache {
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
		log.Fatalf("Faild to create Databricks Workspace Files Client: %v", err)
	}

	// Set up Root node
	root, err := wsfsfuse.NewRootNode(wfclient, diskCache, "/")
	if err != nil {
		log.Fatalf("Faild to create root node: %v", err)
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
			AllowOther: true,
			Name:       "wsfs",
			FsName:     "wsfs",
		},
	}
	opts.Debug = *debug

	server, err := fs.Mount(flag.Arg(0), root, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	logging.Debugf("Mounted Databricks workspace on %s", flag.Arg(0))
	logging.Debugf("Press Ctrl+C to unmount")

	server.Wait()
}
