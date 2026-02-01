# Databricks Workspace File System (wsfs)

A FUSE-based file system to interact with Databricks workspace files and directories as if they were part of the local file system.

## Features

- [x] Mount Databricks workspace.
- [x] List files and directories.
- [x] Read files.
- [x] Write files.
- [x] Make files and directories.
- [x] Delete files and directories.
- [x] Support for filesystem operations (`Rename`, `Fsync` and `Setattr`).
- [x] Disk-based file caching with TTL and LRU eviction for faster access.

Notes:
- `Setattr` currently supports size changes (truncate) and mtime updates. atime-only updates return ENOTSUP. chmod/chown also return ENOTSUP.
- Vim saves are verified in the test suite.

## Current behavior & limitations

- Permissions are not enforced; `Access` currently allows all callers.
- `Statfs` returns synthetic but stable values.
- `Open` with write/truncate uses direct I/O; reads use the in-memory buffer after first fetch.
- `Flush/Fsync/Release` write back dirty buffers; `Release` also drops the in-memory buffer.
- atime-only updates are ENOTSUP; chmod/chown are ENOTSUP.

Behavior details: see `docs/behavior.md`.

## TODO: Improve wsfs toward an ideal FUSE filesystem

These are the most important gaps discovered so far (see `docs/databricks-api-survey.md` for API details).

### Data path & performance
- Support large file uploads via `workspace-files/new-files` + signed URL upload (currently returns SAS URL; upload step fails with 403 and needs correct method/headers/permissions).
- Determine correct request schema for `workspace-files/write-files` (current attempts return BAD_REQUEST).
- Implement smarter caching (read cache + write-back with consistency/eviction).

### Semantics & compatibility
- Decide on xattr behavior (`Get/Set/List/Remove` → `ENOTSUP` or emulation).
- Decide on symlink/hardlink/device support (`Readlink`, `Symlink`, `Link`, `Mknod` likely `ENOTSUP`).
- Add advisory locks (`Getlk/Setlk/Setlkw`) or return `ENOTSUP`.
- Add `Lseek`/sparse support or return `ENOTSUP`.

### Lifecycle & correctness
- `Statx` mapping to `workspace-files/object-info` fields.

### API discovery gaps
- Figure out required params for `workspace-files/get-safe-flags`.
- Validate `notebooks/sync-notebooks-to-wsfs` side effects and response contract.

## Distribution & Development Experience

- [ ] Automate release builds using GitHub Actions.
- [ ] Support installation via Homebrew (`brew install`).
- [ ] Expand unit and integration tests to ensure stability.
- [ ] Allow users to develop on Databricks directly from VSCode by running wsfs within a Remote Container.

## Usage

1. Install FUSE on your system if you haven't already.
2. Set the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables with your Databricks workspace URL and personal access token.

```bash
$ cat .env
export DATABRICKS_HOST=<your-databricks-workspace-url>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

3. Run the application with the desired mount point.

```bash
$ source .env
$ go build -o wsfs
$ ./wsfs <mount-point>
```

4. Access your Databricks workspace files through the mount point.

```bash
$ cd <mount-point>
$ ls
Repos  Shared  Users
```

## Cache Configuration

wsfs includes a disk-based caching system to improve read performance and reduce bandwidth usage. The cache uses SHA256 hashing for file naming and implements both TTL (Time To Live) and LRU (Least Recently Used) eviction strategies.

### Cache Options

```bash
# Enable cache (default: true)
$ ./wsfs --cache=true <mount-point>

# Disable cache
$ ./wsfs --cache=false <mount-point>

# Custom cache directory (default: /tmp/wsfs-cache)
$ ./wsfs --cache-dir=/path/to/cache <mount-point>

# Custom cache size in GB (default: 10GB)
$ ./wsfs --cache-size=5 <mount-point>

# Custom cache TTL (default: 24h)
$ ./wsfs --cache-ttl=1h <mount-point>
$ ./wsfs --cache-ttl=30m <mount-point>
```

### Cache Behavior

- **Cache Hit**: When reading a file, if a valid cache entry exists (not expired, remote file not modified), data is read from local cache.
- **Cache Miss**: When no valid cache entry exists, data is fetched from Databricks and cached for future reads.
- **Automatic Invalidation**: Cache entries are automatically invalidated when:
  - Remote file is modified (detected via modification time)
  - File is deleted or renamed locally
  - TTL expires
  - Cache is full and LRU eviction is triggered
- **Write-Through**: When writing files, data is written to Databricks and the cache is updated on successful flush.

### Cache Monitoring

When running with `--debug` flag, cache operations are logged:

```bash
$ ./wsfs --debug --cache=true <mount-point>
# Look for log messages:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
# - "Cached file /path/to/file (1234 bytes)"
```

## Testing

wsfs includes comprehensive test suites covering FUSE operations, caching behavior, and integration with Databricks. All tests are designed to run in Docker for consistency and portability.

### Quick Start

```bash
# Run standard FUSE tests
$ docker compose run --rm --build wsfs-test

# Run comprehensive cache tests (includes all cache-related tests)
$ docker compose run --rm --build wsfs-cache-test
```

**Prerequisites:**
- Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env` file
- Docker with FUSE support

### Test Suite Overview

#### 1. Unit Tests (Go)

**Location:** `internal/filecache/disk_cache_test.go`

```bash
$ go test ./internal/filecache/...
```

Tests for disk cache implementation:
- Cache entry storage and retrieval
- TTL expiration
- LRU eviction
- Cache invalidation
- Concurrent access handling

**Coverage:** 13 test cases, 100% code coverage for disk cache

---

#### 2. Basic FUSE Operations Tests

**Script:** `scripts/fuse_test.sh`
**Docker:** `docker compose run --rm wsfs-test`

Tests fundamental FUSE filesystem operations:

| Test Category | Operations Tested |
|--------------|-------------------|
| **File Operations** | Create, read, write, append, delete |
| **Directory Operations** | Create, list, delete (empty and non-empty) |
| **Metadata Operations** | Stat, chmod, touch, truncate |
| **Advanced Operations** | Rename, symlink, fsync |
| **Vim Compatibility** | Vim save workflow (backup files, atomic writes) |

**Total:** 10+ test categories

**Run directly on Linux:**
```bash
$ sudo apt-get install -y fuse3 vim
$ echo 'user_allow_other' | sudo tee -a /etc/fuse.conf
$ mkdir -p /mnt/wsfs
$ go build -o tmp/wsfs ./cmd/wsfs
$ ./tmp/wsfs /mnt/wsfs &
$ ./scripts/fuse_test.sh /mnt/wsfs
$ fusermount3 -u /mnt/wsfs
```

---

#### 3. Large File Tests

**Script:** `scripts/large_file_test.sh`

Tests handling of large files:
- **10MB file**: Read/write/verify (always run)
- **50MB file**: Optional (set `LARGE_FILE_TEST=1`)
- **100MB file**: Optional (set `LARGE_FILE_TEST=2`)
- **Streaming reads**: Chunked reading (1MB blocks)

**Usage:**
```bash
$ ./scripts/large_file_test.sh /mnt/wsfs

# Test with larger files
$ LARGE_FILE_TEST=1 ./scripts/large_file_test.sh /mnt/wsfs  # 50MB
$ LARGE_FILE_TEST=2 ./scripts/large_file_test.sh /mnt/wsfs  # 100MB
```

**Verification:** Uses `diff` and MD5 checksums

---

#### 4. Cache Tests

##### 4.1 Basic Cache Operations

**Script:** `scripts/cache_test.sh`
**Tests:** 9 test categories

| Test | Description |
|------|-------------|
| **Basic Cache Hit/Miss** | First read (miss) → cache population → second read (hit) |
| **Cache Invalidation on Write** | File modification invalidates cache |
| **Cache Invalidation on Delete** | File deletion removes cache entry |
| **Cache Invalidation on Rename** | Old cache invalidated, new path starts fresh |
| **Cache Persistence** | Multiple files cached and retrieved correctly |
| **Large File Caching** | 1MB file cached with hash verification |
| **Concurrent File Access** | 10 parallel reads without corruption |
| **Cache with Truncate** | Truncation updates cache correctly |
| **Cache Directory Operations** | Directory deletion cleans up cached files |

**Usage:**
```bash
$ ./scripts/cache_test.sh /mnt/wsfs [cache_dir] [log_file]
```

---

##### 4.2 Cache Synchronization Tests

**Script:** `scripts/cache_sync_test.sh`
**Tests:** 4 test categories

| Test | Description |
|------|-------------|
| **Detect Remote File Modification** | Detects when remote file changes via modTime comparison |
| **Cache Behavior After Flush** | Writes are properly flushed and readable |
| **Multiple File Modifications** | Batch remote modifications are detected |
| **Cache Behavior with Touch** | Touch updates mtime without changing content |

**Usage:**
```bash
$ export DATABRICKS_HOST=https://your-workspace.databricks.com
$ export DATABRICKS_TOKEN=your-token
$ ./scripts/cache_sync_test.sh /mnt/wsfs
```

**Note:** Modifies files via Databricks API to simulate remote changes

---

##### 4.3 Databricks CLI Verification Tests

**Script:** `scripts/databricks_cli_verification_test.sh`
**Tests:** 8 verification scenarios

Verifies wsfs operations against official Databricks CLI to ensure correct synchronization:

| Test | Verification Method |
|------|---------------------|
| **File Creation** | `databricks workspace export` matches wsfs content |
| **File Modification** | CLI shows updated content after wsfs write |
| **File Rename** | Old path doesn't exist, new path exists via CLI |
| **File Delete** | CLI confirms file no longer exists |
| **Directory Operations** | `databricks workspace list` shows correct structure |
| **Directory Rename** | CLI reflects renamed directory |
| **Directory Delete** | CLI confirms directory removal |
| **Large File (1MB)** | MD5 checksum matches between wsfs and CLI download |

**Requirements:**
- Databricks CLI installed in Docker image
- Valid `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

**Usage:**
```bash
$ export DATABRICKS_HOST=https://your-workspace.databricks.com
$ export DATABRICKS_TOKEN=your-token
$ ./scripts/databricks_cli_verification_test.sh /mnt/wsfs
```

**Purpose:** Ensures wsfs maintains 100% compatibility with official Databricks workspace state

---

##### 4.4 Comprehensive Cache Integration Tests

**Script:** `scripts/docker_cache_test.sh`
**Docker:** `docker compose run --rm wsfs-cache-test`

Runs all cache tests across multiple configurations:

| Configuration | Settings | Tests Run |
|--------------|----------|-----------|
| **Test 1: Default Cache** | 10GB size, 24h TTL | All 3 cache test suites |
| **Test 2: Cache Disabled** | No caching | Basic operations only |
| **Test 3: Short TTL** | 5 second TTL | TTL expiration behavior |
| **Test 4: Small Cache** | 1MB size | LRU eviction with 5×300KB files |

**Total execution time:** ~2-3 minutes

**What it verifies:**
- ✅ Cache hit/miss operations (9 tests)
- ✅ Cache invalidation on write/delete/rename
- ✅ Remote file modification detection (4 tests)
- ✅ Databricks CLI compatibility (8 tests)
- ✅ TTL-based expiration
- ✅ LRU-based eviction
- ✅ Cache disable mode

---

### Test Results & Logs

**Cache Statistics:**
```bash
# View cache directory contents
$ ls -lh /tmp/wsfs-cache/

# Check cache size
$ du -sh /tmp/wsfs-cache/

# Count cache entries
$ find /tmp/wsfs-cache -type f | wc -l
```

**Debug Logging:**
```bash
$ ./wsfs --debug --cache=true /mnt/wsfs
# Look for:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
# - "Cached file /path/to/file (1234 bytes)"
# - "Updated cache after flush for /path/to/file"
```

---

### Test Documentation

For detailed test documentation, see:
- [`docs/cache-testing.md`](docs/cache-testing.md) - Cache testing methodology
- [`docs/behavior.md`](docs/behavior.md) - Expected FUSE behavior
- [`docs/databricks-api-survey.md`](docs/databricks-api-survey.md) - API verification details

---

### Continuous Integration

All tests are designed to run in CI environments:
- Docker-based execution for consistency
- No manual intervention required
- Clear pass/fail indicators
- Detailed error reporting

**Exit codes:**
- `0` - All tests passed
- `1` - Test failure (with specific error message)

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
