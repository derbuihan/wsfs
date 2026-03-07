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
- [x] Always-on metadata and disk caching for faster directory browsing and file reads.
- [x] Expose Databricks notebooks as source files (`.py`, `.sql`, `.scala`, `.R`) based on notebook language.

Notes:
- `Setattr` currently supports size changes (truncate) and mtime updates. atime-only updates return ENOTSUP. chmod/chown also return ENOTSUP.
- Vim saves are verified in the test suite.
- Notebooks are shown as source files by default. `.ipynb` appears only as a fallback when the preferred source name collides with an exact workspace entry or when notebook language is unknown.

## Current behavior & limitations

- Permissions are not enforced; `Access` currently allows all callers.
- `Statfs` returns synthetic but stable values.
- `Open` with write/truncate uses direct I/O; reads use the in-memory buffer after first fetch.
- `Flush/Fsync/Release` write back dirty buffers; `Release` also drops the in-memory buffer.
- atime-only updates are ENOTSUP; chmod/chown are ENOTSUP.
- Creating `foo.py` creates a Python notebook named `foo` in Databricks. Creating `foo.ipynb` creates a regular workspace file named `foo.ipynb`.

Behavior details: see `docs/behavior.md`.

## Usage

1. Install FUSE on your system if you haven't already.
2. Set the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables with your Databricks workspace URL and personal access token.

```bash
$ cat .env
export DATABRICKS_HOST=<your-databricks-workspace-url>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

3. Run the application with the desired mount point (manual runs expect `.env` in the repo root).

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

$ ls Users/user@example.com
analysis.py  dashboard.sql  regular-file.txt
```

## Debian/Ubuntu (.deb)

1. Download the latest `.deb` from GitHub Releases and install it.

```bash
$ sudo dpkg -i wsfs_*.deb
```

2. Create a systemd user env file (no `export` lines) and enable the service.

```bash
$ mkdir -p ~/.config/wsfs
$ cp /usr/share/doc/wsfs/wsfs.env.example ~/.config/wsfs/dev.env
$ $EDITOR ~/.config/wsfs/dev.env
$ systemctl --user daemon-reload
$ systemctl --user enable --now wsfs@dev
```

**Update:** download a newer `.deb` and run `dpkg -i` again (manual updates).

## Security Considerations

> **Important:** wsfs is designed for single-user development environments.

### Single-User Environment

By default, wsfs restricts access to the mount owner. This is enforced by the kernel (without `--allow-other`) and by wsfs's `Access()` check. This means:

- Only the user who mounted wsfs can read/write files through the mount point
- Operations use the Databricks token owner's permissions
- Access is restricted by UID when `--allow-other` is **not** set

**Recommendation:** Use wsfs only on machines where you are the sole user, or explicitly enable access for others with `--allow-other` if you understand the risks.

### The `--allow-other` Flag

By default, only the user who mounted wsfs can access the mount point. The `--allow-other` flag allows other local users to access it, and wsfs does not restrict access by UID when it is enabled.

**Warning:** Do NOT use `--allow-other` unless absolutely necessary. When enabled:
- All local users gain access to your Databricks workspace
- They can read, write, and delete files using your token's permissions
- There is no way to restrict access to specific users

### Cache Security

wsfs creates cache files with restricted permissions:
- Cache directory: `0700` (owner only)
- Cache files: `0600` (owner read/write only)

The cache is always enabled. By default, wsfs stores disk cache data in `$XDG_CACHE_HOME/wsfs`; if `XDG_CACHE_HOME` is unset, it falls back to `~/.cache/wsfs`.

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

## Zero-Config Cache

wsfs always uses two cache layers:
- A metadata cache for directory listings, lookups, and short-lived negative entries
- A disk-backed content cache for file reads

There are no cache tuning flags. The goal is that `./wsfs <mount-point>` is enough for normal use.

### Cache Behavior

- Directory metadata is reused for short TTL windows so shells and editors do not re-fetch the same listings on every open.
- File contents are cached on disk after the first read and reused until the entry is invalidated or evicted.
- Local write, rename, delete, and mkdir/rmdir paths invalidate related metadata and content cache entries.
- Disk cache entries are stored under `$XDG_CACHE_HOME/wsfs`, or `~/.cache/wsfs` when `XDG_CACHE_HOME` is unset.
- Cache directory permissions are `0700`; cache files are `0600`.

### Cache Monitoring

When running with `--debug`, cache activity is logged:

```bash
$ ./wsfs --debug <mount-point>
# Look for log messages such as:
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

# VSCode integration tests via Docker (Core dev loop)
./scripts/run_vscode_tests_docker.sh

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
| **Cache tests** | `scripts/tests/cache_test.sh` | Default cache population, invalidation, remote refresh checks |
| **Stress tests** | `scripts/tests/stress_test.sh` | Concurrent access, rapid truncate, rename |
| **VSCode core dev loop** | `scripts/run_vscode_tests_docker.sh` | One VSCode session covering edit/save/search/rename/terminal/Python run |

### Test Options

```bash
# Run specific test suite
./scripts/run_tests_docker.sh --fuse-only
./scripts/run_tests_docker.sh --cache-only
./scripts/run_tests_docker.sh --stress-only

# Skip specific tests
./scripts/run_tests_docker.sh --skip-stress

# Rebuild Docker image
./scripts/run_tests_docker.sh --build
```

### Debug Logging

```bash
./wsfs --debug /mnt/wsfs
# Look for:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
```

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
