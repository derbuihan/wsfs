# CLAUDE.md

This file provides guidance to Claude Code when working with this repository.

## Project Overview

wsfs is a FUSE-based filesystem that mounts Databricks workspace as a local filesystem. Written in Go using go-fuse v2.

## Quick Reference

### Build & Run
```bash
# Build
go build -o wsfs ./cmd/wsfs

# Run (requires DATABRICKS_HOST and DATABRICKS_TOKEN env vars)
./wsfs /mnt/wsfs
./wsfs --cache=true --cache-dir=/tmp/wsfs-cache /mnt/wsfs
./wsfs --debug /mnt/wsfs

# Unmount
fusermount3 -u /mnt/wsfs  # Linux
umount /mnt/wsfs          # macOS
```

### Testing
```bash
# Go unit tests (no .env required)
go test ./...
go test -race ./...

# Integration tests (requires .env with DATABRICKS_HOST/TOKEN)
./scripts/run_tests_docker.sh
./scripts/run_tests_docker.sh --fuse-only
./scripts/run_tests_docker.sh --cache-only
```

## Architecture

```
FUSE Layer (go-fuse v2)
    │
    ▼
WSNode (internal/fuse/node.go)
    │
    ├─→ DiskCache (internal/filecache/) - LRU + TTL
    │
    ▼
WorkspaceFilesClient (internal/databricks/client.go)
    │
    ▼
Databricks Workspace APIs
```

### Key Design Patterns

1. **Fallback Strategy**
   - Read: signed URL → workspace.Export
   - Write: new-files → import-file

2. **Two-Level Caching**
   - Metadata cache: 60s TTL (internal/metacache/)
   - Data cache: Disk-based LRU + TTL (internal/filecache/)

3. **Path Conversion**
   - fusePath: User-facing with `.ipynb` suffix for notebooks
   - remotePath: Databricks API without suffix
   - Conversion centralized in `internal/pathutil/`

## Core Files

| File | Purpose |
|------|---------|
| `cmd/wsfs/main.go` | Entry point, CLI flags |
| `internal/fuse/node.go` | FUSE operations (WSNode) |
| `internal/databricks/client.go` | Databricks API wrapper |
| `internal/pathutil/pathutil.go` | Path conversion utilities |
| `internal/filecache/disk_cache.go` | Disk cache with LRU/TTL |
| `internal/metacache/cache.go` | Metadata cache |

## Testing Reference

| Change Area | Unit Test | Integration Test |
|-------------|-----------|------------------|
| FUSE ops | `internal/fuse/node_test.go` | `scripts/tests/fuse_test.sh` |
| API calls | `internal/databricks/client_test.go` | `scripts/tests/fuse_test.sh` |
| Disk cache | `internal/filecache/disk_cache_test.go` | `scripts/tests/cache_test.sh` |
| Path conversion | `internal/pathutil/pathutil_test.go` | N/A |

## Code Change Guidelines

### After Significant Changes
```bash
go test ./...                    # Unit tests
./scripts/run_tests_docker.sh   # Integration tests
```

### Adding New Features
1. Add method to appropriate package
2. Implement fallback strategy if calling external APIs
3. Add unit test in `*_test.go`
4. Test with both `--cache=true` and `--cache=false`

## Known Limitations

- `Statfs` returns synthetic values (not actual workspace capacity)
- atime-only updates return `ENOTSUP`
- chmod/chown return `ENOTSUP`
- `--allow-other` enables UID-based access restriction
