package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/workspace"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatalf("Usage: %s MOUNTPOINT", os.Args[0])
	}

	// Set up Databricks client
	w, err := databricks.NewWorkspaceClient()
	if err != nil {
		log.Fatalf("Failed to create Databricks client: %v", err)
	}
	me, err := w.CurrentUser.Me(context.Background())
	if err != nil {
		log.Fatalf("Failed to get current user: %v", err)
	}
	log.Printf("Hello, %s! Mounting your Databricks workspace...", me.DisplayName)

	// Set up Databricks FS client
	wfclient, err := NewWorkspaceFilesClient(w)
	if err != nil {
		log.Fatalf("Faild to create Databricks Workspace Files Client: %v", err)
	}

	req := NewListFilesRequest("/")
	ctx := context.Background()
	res, err := wfclient.ListFiles(ctx, req)
	if err != nil {
		log.Fatalf("Failed to list files: %v", err)
	}
	log.Printf("Listed %d files in the root directory", len(res.Objects))

	// Set up FUSE filesystem
	root := &WSNode{
		wfclient:   wfclient,
		path:       "/",
		objectType: workspace.ObjectTypeDirectory,
	}

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
	log.Printf("Mounted Databricks workspace on %s", flag.Arg(0))
	log.Println("Press Ctrl+C to unmount")

	server.Wait()
}
