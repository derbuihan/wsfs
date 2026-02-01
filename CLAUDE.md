# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

wsfs (Databricks Workspace File System) is a FUSE-based filesystem that mounts Databricks workspace as a local filesystem. Written in Go using go-fuse v2, it enables reading/writing Databricks workspace files as if they were local files.

## Build and Development Commands

### Building
```bash
# Build the binary
go build -o wsfs ./cmd/wsfs

# Build for Docker testing
docker compose build
```

### Running
```bash
# Required environment variables
export DATABRICKS_HOST=https://your-workspace.databricks.com
export DATABRICKS_TOKEN=your-personal-access-token

# Mount workspace
./wsfs /mnt/wsfs

# Mount with cache options
./wsfs --cache=true --cache-dir=/tmp/wsfs-cache --cache-size=10 --cache-ttl=24h /mnt/wsfs

# Mount with debug logging
./wsfs --debug /mnt/wsfs

# Unmount
fusermount3 -u /mnt/wsfs  # Linux
umount /mnt/wsfs          # macOS
```

### Testing

Tests are designed to run in Docker for consistency. Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env` file.

```bash
# Run all Go unit tests
go test ./...

# Run specific package tests
go test ./internal/filecache/...

# Run basic FUSE operation tests (Docker)
docker compose run --rm --build wsfs-test

# Run comprehensive cache tests (Docker)
docker compose run --rm --build wsfs-cache-test

# Run specific test script directly (requires mounted filesystem)
./scripts/fuse_test.sh /mnt/wsfs
./scripts/cache_test.sh /mnt/wsfs
./scripts/large_file_test.sh /mnt/wsfs
./scripts/databricks_cli_verification_test.sh /mnt/wsfs
```

## Architecture Overview

### Component Layers

```
┌─────────────────────────────────────┐
│   FUSE Layer (go-fuse v2)           │  User-space filesystem operations
│   internal/fuse/node.go             │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   WSNode (FUSE operations handler)  │  Implements FUSE interface methods
│   - Getattr, Setattr                │  (Read, Write, Open, Flush, etc.)
│   - Read, Write, Open, Release      │
│   - Create, Unlink, Rename          │
│   - Mkdir, Rmdir, Readdir           │
└──────────┬──────────────────────────┘
           │
           ├──────────────────────────────────┐
           ▼                                  ▼
┌──────────────────────────┐      ┌──────────────────────────┐
│   FileBuffer             │      │   DiskCache              │
│   internal/buffer/       │      │   internal/filecache/    │
│                          │      │                          │
│   - In-memory data       │      │   - SHA256-based paths   │
│   - Dirty flag           │      │   - LRU + TTL eviction   │
└──────────────────────────┘      │   - Size limit: 10GB     │
                                  └──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   WorkspaceFilesClient              │  Databricks API wrapper
│   internal/databricks/client.go     │
│                                     │
│   Read: signed URL → Export fallback│
│   Write: new-files → write-files    │
│          → import-file fallback     │
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────────────────────┐
│   Databricks Workspace APIs         │
│   - workspace-files/object-info     │
│   - workspace-files/list-files      │
│   - workspace-files/import-file     │
│   - workspace.Export                │
└─────────────────────────────────────┘
```

### Key Design Patterns

#### 1. Layered Fallback Strategy
The Databricks client tries optimized APIs first, then falls back to legacy APIs:
- **Read**: signed URL download (fast) → workspace.Export (base64, slower)
- **Write**: new-files/write-files (experimental) → import-file (stable)

Implementation in `internal/databricks/client.go`:
- `ReadAll()` - lines 223-251
- `Write()` - lines 318-344

#### 2. Two-Level Caching
- **Metadata cache** (`internal/cache/cache.go`): 60s TTL for file stat info
- **Data cache** (`internal/filecache/disk_cache.go`): Disk-based with LRU + TTL

Data flow in `internal/fuse/node.go::ensureDataLocked()` (lines 78-125):
```
Read request → Check in-memory buffer → Check disk cache → Fetch from remote
```

#### 3. In-Memory Buffer with Dirty Tracking
Files are loaded into memory (`FileBuffer`) on first access and kept until `Release()`:
- Writes mark buffer as dirty
- `Flush()` writes dirty data back to Databricks
- `Release()` flushes and drops in-memory buffer
- Cache is updated after successful flush

#### 4. Interface Abstraction
Testing is enabled through thin interfaces:
- `workspaceClient` interface (lines 104-108 in client.go) - wraps Databricks SDK
- `apiDoer` interface (lines 96-100 in client.go) - wraps HTTP client
- `WorkspaceFilesAPI` interface - wraps all workspace operations for FUSE nodes

## File Locations Reference

### Core Implementation
- `cmd/wsfs/main.go` - Entry point, CLI flags, initialization
- `internal/fuse/node.go` - WSNode struct, all FUSE operations (650+ lines)
- `internal/databricks/client.go` - WorkspaceFilesClient with fallback logic
- `internal/buffer/file_buffer.go` - FileBuffer struct (Data + Dirty flag)
- `internal/filecache/disk_cache.go` - DiskCache with LRU/TTL eviction
- `internal/cache/cache.go` - In-memory metadata cache with 60s TTL

### Testing
- `internal/filecache/disk_cache_test.go` - Go unit tests (13 tests, 100% coverage)
- `scripts/fuse_test.sh` - Basic FUSE operations (10+ categories)
- `scripts/cache_test.sh` - Cache hit/miss/invalidation (9 categories)
- `scripts/cache_sync_test.sh` - Remote modification detection (4 categories)
- `scripts/databricks_cli_verification_test.sh` - Databricks CLI comparison (8 scenarios)
- `scripts/docker_cache_test.sh` - Integration tests (4 configurations)
- `scripts/large_file_test.sh` - Large file handling (10MB/50MB/100MB)

## Important Implementation Details

### FUSE Operation Flow: Read
1. User reads file → FUSE Read() called
2. `WSNode.Read()` acquires lock, calls `ensureDataLocked()`
3. Check if data in memory buffer → return if present
4. Check disk cache (`DiskCache.Get()`) → return if valid (TTL not expired, remote not modified)
5. Fetch from remote (`WorkspaceFilesClient.ReadAll()`)
6. Try signed URL download → fallback to workspace.Export
7. Store in disk cache and memory buffer
8. Return data to FUSE

### FUSE Operation Flow: Write
1. User writes file → FUSE Write() called
2. `WSNode.Write()` acquires lock, ensures data loaded
3. Modify in-memory buffer, mark as dirty
4. User closes file → FUSE Flush() then Release() called
5. `flushLocked()` writes buffer to Databricks (`WorkspaceFilesClient.Write()`)
6. Try new-files → write-files → import-file (fallback chain)
7. Refresh file metadata with `Stat()`
8. Update disk cache with new content
9. `Release()` drops in-memory buffer

### Stat Information Caching
`WorkspaceFilesClient.Stat()` (lines 136-164 in client.go) maintains a metadata cache:
- Caches `WSFileInfo` including signed URLs and headers
- 60-second TTL in `internal/cache/cache.go`
- Signed URLs are obtained from `workspace-files/object-info` API
- Cache is invalidated on Write, Delete, Rename operations

### Cache File Naming
Disk cache uses SHA256 hash of remote path as filename (`disk_cache.go:302-307`):
- Remote: `/Users/user@example.com/test.txt`
- Cached: `/tmp/wsfs-cache/a3b2c1d4e5f6...`
- Avoids path length limits and special character issues

### go-fuse Interface Requirements
`WSNode` implements 20+ go-fuse interfaces (lines 30-49 in node.go):
- Basic: `NodeGetattrer`, `NodeSetattrer`, `NodeReader`, `NodeWriter`
- Files: `NodeOpener`, `NodeCreater`, `NodeUnlinker`, `NodeFlusher`, `NodeFsyncer`, `NodeReleaser`
- Directories: `NodeReaddirer`, `NodeLookuper`, `NodeOpendirer`, `NodeMkdirer`, `NodeRmdirer`
- Advanced: `NodeRenamer`, `NodeAccesser`, `NodeStatfser`, `NodeOnForgetter`

Each interface method must return `syscall.Errno` (0 for success) and handle context cancellation.

## Common Modification Patterns

### Adding a New FUSE Operation
1. Check if go-fuse interface exists (search `github.com/hanwen/go-fuse/v2/fs`)
2. Add interface assertion at top of `internal/fuse/node.go`
3. Implement method on `WSNode` struct
4. Add test case to appropriate script in `scripts/`

### Adding a New Databricks API Call
1. Add method to `WorkspaceFilesClient` in `internal/databricks/client.go`
2. Consider adding to `WorkspaceFilesAPI` interface if needed by FUSE nodes
3. Implement fallback strategy (try new API → fallback to old API)
4. Add debug logging for both paths
5. Invalidate metadata cache if operation modifies state

### Modifying Cache Behavior
- **Metadata cache**: Edit `internal/cache/cache.go` (simple TTL-based map)
- **Data cache**: Edit `internal/filecache/disk_cache.go` (LRU + TTL with disk storage)
- **Cache invalidation**: Search for `cache.Invalidate()` and `diskCache.Delete()` calls
- Always test with both `--cache=true` and `--cache=false`

## Known Limitations

From [README.md](README.md:20-27):
- Permissions are not enforced; `Access` currently allows all callers
- `Statfs` returns synthetic but stable values
- atime-only updates return `ENOTSUP`
- chmod/chown return `ENOTSUP`
- `new-files` signed URL upload may return 403 (uses fallback)
- `write-files` correct request format unknown (uses fallback)

## Task Planning Reference

The repository follows a phased development approach documented in [Task.md](Task.md):
- **Phase R**: Refactoring (completed) - Interface boundaries, testability
- **Phase 1**: Compatibility (completed) - go-fuse interface implementations
- **Phase 2**: Data path (completed) - Signed URL read/write with fallbacks
- **Phase 3**: Caching (completed) - Disk-based cache with LRU + TTL
- **Phase 4**: Stability (in progress) - Stress testing, concurrent access
- **Phase 5**: Documentation & release quality (pending)

When working on new features, follow the "fail-safe" principle: new functionality must have fallback paths and not break existing tests.
