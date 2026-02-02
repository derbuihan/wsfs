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

## Security Considerations

> **Important:** wsfs is designed for single-user development environments.

### Single-User Environment

wsfs does not implement access control. The `Access()` system call always permits all operations. This means:

- All local users can read/write files through the mount point
- Operations use the Databricks token owner's permissions
- There is no UID/GID-based access restriction

**Recommendation:** Use wsfs only on machines where you are the sole user.

### The `--allow-other` Flag

By default, only the user who mounted wsfs can access the mount point. The `--allow-other` flag allows other users to access it.

**Warning:** Do NOT use `--allow-other` unless absolutely necessary. When enabled:
- All local users gain access to your Databricks workspace
- They can read, write, and delete files using your token's permissions
- There is no way to restrict access to specific users

### Cache Security

wsfs creates cache files with restricted permissions:
- Cache directory: `0700` (owner only)
- Cache files: `0600` (owner read/write only)

The default cache location is `/tmp/wsfs-cache`. For sensitive data, consider using a custom location:

```bash
./wsfs --cache-dir=/home/user/.wsfs-cache <mount-point>
```

### Token Security

The `DATABRICKS_TOKEN` environment variable contains sensitive credentials:
- Never commit `.env` files to version control
- Avoid passing tokens via command line arguments (visible in `ps`)
- Consider using Databricks CLI profiles or OAuth for authentication

## Recommended Use Cases

### Recommended

- **Local development** - Edit Databricks notebooks and files from your local IDE
- **CI/CD pipelines** - Temporary mounts for automated workflows
- **VSCode Remote Containers** - Development in isolated container environments

### Not Recommended

- **Shared servers** - Multiple users would share the same Databricks token permissions
- **Production/long-running services** - Not designed for high-availability or concurrent access
- **Sensitive data environments** - Limited access control and audit capabilities
- **With `--allow-other` enabled** - Exposes your Databricks access to all local users

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

wsfs includes comprehensive test suites covering FUSE operations, caching behavior, and stress testing.

### Quick Start

```bash
# Go unit tests (no .env required)
go test ./...

# Integration tests via Docker (Mac)
./scripts/run_tests_docker.sh

# Integration tests on Linux (requires mounted wsfs)
./scripts/run_tests.sh /mnt/wsfs
```

**Prerequisites:**
- Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env` file
- Docker with FUSE support (for Mac)

### Test Suites

| Suite | Script | Description |
|-------|--------|-------------|
| **FUSE tests** | `scripts/tests/fuse_test.sh` | File/directory operations, vim compatibility |
| **Cache tests** | `scripts/tests/cache_test.sh` | Cache hit/miss, invalidation, TTL behavior |
| **Stress tests** | `scripts/tests/stress_test.sh` | Concurrent access, rapid truncate, rename |
| **Config tests** | `scripts/tests/cache_config_test.sh` | Cache disabled mode, permissions, short TTL |

### Test Options

```bash
# Run specific test suite
./scripts/run_tests_docker.sh --fuse-only
./scripts/run_tests_docker.sh --cache-only
./scripts/run_tests_docker.sh --stress-only

# Skip specific tests
./scripts/run_tests_docker.sh --skip-stress
./scripts/run_tests_docker.sh --skip-config-test

# Rebuild Docker image
./scripts/run_tests_docker.sh --build
```

### Debug Logging

```bash
./wsfs --debug --cache=true /mnt/wsfs
# Look for:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
```

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
