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

### Docker (recommended on macOS)

This uses `docker-compose.yml` and runs the test script (`scripts/fuse_test.sh`) inside a privileged container with FUSE enabled. The Docker image includes Vim so the save-path tests run in CI.

```bash
# Run standard FUSE tests
$ docker compose run --rm --build wsfs-test

# Run comprehensive cache tests
$ docker compose run --rm --build wsfs-cache-test
```

Notes:
- The container reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from `.env` via `env_file`.
- `.env` is intentionally excluded from Git. Do not commit secrets.

### Cache Tests

The cache test suite (`scripts/cache_test.sh` and `scripts/cache_sync_test.sh`) verifies:

1. **Basic Cache Operations**
   - Cache hit/miss behavior
   - Cache persistence across reads
   - Concurrent file access

2. **Cache Invalidation**
   - Invalidation on file write
   - Invalidation on file delete
   - Invalidation on file rename
   - Invalidation on truncate

3. **Cache Synchronization**
   - Detection of remote file modifications via Databricks API
   - Cache behavior after flush operations
   - Multiple file modifications

4. **Eviction Strategies**
   - TTL-based expiration (tested with 5s TTL)
   - LRU-based eviction (tested with 1MB cache size)
   - Cache disabled mode

The comprehensive cache test (`docker_cache_test.sh`) runs four test configurations:
- Default settings (10GB cache, 24h TTL)
- Cache disabled mode
- Short TTL (5 seconds)
- Small cache size (1MB, triggers LRU eviction)

### Linux (direct)

If you want to run tests directly on Linux without Docker:

```bash
$ sudo apt-get update
$ sudo apt-get install -y fuse3
$ sudo apt-get install -y vim
$ echo 'user_allow_other' | sudo tee -a /etc/fuse.conf
$ mkdir -p /mnt/wsfs
$ go build -o tmp/wsfs
$ ./tmp/wsfs /mnt/wsfs &
$ ./scripts/fuse_test.sh /mnt/wsfs
$ fusermount3 -u /mnt/wsfs
```

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
