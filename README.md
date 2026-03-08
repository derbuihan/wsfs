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
- `Setattr` supports size changes. Timestamp-only updates on existing files (`atime`, `mtime`, or both) return `ENOTSUP`, while the initial post-create timestamp sync for a brand-new empty file is accepted as a compatibility no-op so `touch new-file` works. `chmod` succeeds as a compatibility no-op, while `chown` still returns `ENOTSUP`.
- Vim saves are verified in the test suite.
- Notebooks are shown as source files by default. `.ipynb` appears only as a fallback when the preferred source name collides with an exact workspace entry or when notebook language is unknown.

## Current behavior & limitations

- Without `--allow-other`, the mount is owner-only. With `--allow-other`, other local users can access the mount through the same Databricks token.
- `stat(2)` reports the mount owner's UID/GID and synthetic mode bits (`0644` files, `0755` directories).
- `Statfs` returns synthetic but stable values.
- Read-only opens on clean regular files revalidate remote metadata and drop stale clean cache state when the remote file changed.
- `Flush`/`Fsync`/`Release` write back dirty buffers; `Release` also drops clean in-memory buffers after the last close.
- Creating `foo.py` creates a Python notebook named `foo` in Databricks. Creating `foo.ipynb` creates a regular workspace file named `foo.ipynb`.

Behavior details: see `docs/behavior.md`.

## Usage

For source-checkout development, use the Docker shell wrapper on both macOS and Linux.
This is the recommended path because it avoids host-side direct-run drift.

1. Install Docker with Compose support.
2. Create a `.env` file with your Databricks workspace URL and token.

```bash
$ cat .env
export DATABRICKS_HOST=<your-databricks-workspace-url>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

3. Start a Docker shell with `wsfs` mounted inside the container at `/mnt/wsfs`.

```bash
$ ./scripts/run_wsfs_docker.sh
```

4. Work with the mounted files inside that shell.

```bash
root@container:/mnt/wsfs$ ls
Repos  Shared  Users

root@container:/mnt/wsfs$ ls Users/user@example.com
analysis.py  dashboard.sql  regular-file.txt
```

Useful variants:

```bash
# Enable debug logging
./scripts/run_wsfs_docker.sh --debug

# Mount a specific Databricks path
./scripts/run_wsfs_docker.sh --remote-path=/Users/user@example.com

# Run a single command instead of opening a shell
./scripts/run_wsfs_docker.sh -- 'find /mnt/wsfs -maxdepth 2 -type f | head'
```

Notes:
- The FUSE mount is inside the container, not directly on the host filesystem.
- This works consistently for macOS and Linux development machines.
- Packaged host-integrated installs are Linux-only; on macOS, use the Docker workflow above.
- For Linux host-integrated installs, prefer the packaged `.deb` + systemd flow below.

## Debian/Ubuntu (.deb)

1. Download the latest Linux `.deb` from GitHub Releases and install it.

```bash
$ sudo apt install ./wsfs_*.deb
```

2. Create a systemd user env file (no `export` lines) and enable the service.

```bash
$ mkdir -p ~/.config/wsfs
$ cp /usr/share/doc/wsfs/wsfs.env.example ~/.config/wsfs/dev.env
$ $EDITOR ~/.config/wsfs/dev.env
$ systemctl --user daemon-reload
$ systemctl --user enable --now wsfs@dev
```

**Update:** download a newer Linux `.deb` and run `apt install ./wsfs_*.deb` again.

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

There are no cache tuning flags. The goal is that `./scripts/run_wsfs_docker.sh` is enough for normal development use.

### Cache Behavior

- Directory metadata is reused for short TTL windows so shells and editors do not re-fetch the same listings on every lookup.
- Clean regular files are revalidated against remote metadata on read-only open.
- If remote metadata changed, wsfs drops the clean buffer, invalidates related metadata/content cache state, and avoids stale kernel page-cache reuse for that open.
- File contents are cached on disk after the first read and reused until the entry is invalidated or evicted.
- Missing or corrupt disk cache files are invalidated and retried from Databricks once instead of immediately surfacing `EIO`.
- Local write, rename, delete, and mkdir/rmdir paths invalidate related metadata and content cache entries.
- Disk cache entries are stored under `$XDG_CACHE_HOME/wsfs`, or `~/.cache/wsfs` when `XDG_CACHE_HOME` is unset.
- Cache directory permissions are `0700`; cache files are `0600`.

### Cache Monitoring

When running with `--debug`, cache activity is logged inside the Docker shell session:

```bash
$ ./scripts/run_wsfs_docker.sh --debug
# Look for log messages such as:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
# - "Cached file /path/to/file (1234 bytes)"
```

## Testing

wsfs includes comprehensive test suites covering FUSE operations, caching behavior, stress testing, and a VSCode core development loop.

### Quick Start

```bash
# Go unit tests (no .env required)
go test ./...

# Open a Docker shell with wsfs mounted inside the container
./scripts/run_wsfs_docker.sh

# Standard integration tests via Docker (macOS and Linux)
./scripts/test_docker.sh

# VSCode integration tests via Docker (Core dev loop)
./scripts/test_vscode_docker.sh
```

**Prerequisites:**
- Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` in `.env` file
- Docker with Compose support and FUSE-capable privileged containers

### Test Suites

| Suite | Script | Description |
|-------|--------|-------------|
| **Mounted test runner** | `scripts/tests/run.sh` | Runs the shell suites against an already-mounted wsfs filesystem |
| **FUSE tests** | `scripts/tests/fuse_test.sh` | File/directory operations, vim compatibility, timestamp-only `Setattr` expectations |
| **Cache tests** | `scripts/tests/cache_test.sh` | Default cache population, invalidation, and out-of-band remote refresh checks |
| **Stress tests** | `scripts/tests/stress_test.sh` | Concurrent access, rapid truncate, rename |
| **Security / allow-other** | `scripts/tests/security_test.sh` | Validates `--allow-other` exposure semantics with a second local user |
| **Docker shell** | `scripts/run_wsfs_docker.sh` | Common Docker wrapper that builds, mounts, and runs a shell or command |
| **Docker integration wrapper** | `scripts/test_docker.sh` | Runs the standard integration suites, including a separate `--allow-other` security stage |
| **VSCode core dev loop** | `scripts/test_vscode_docker.sh` | Runs the VSCode E2E project in `scripts/tests/vscode/` |

### Test Options

```bash
# Open an interactive shell with wsfs mounted inside Docker
./scripts/run_wsfs_docker.sh

# Run shell suites against an existing mount
./scripts/tests/run.sh /mnt/wsfs --fuse-only

# Run specific Docker-backed test suites
./scripts/test_docker.sh --fuse-only
./scripts/test_docker.sh --cache-only
./scripts/test_docker.sh --stress-only

# Skip specific suites
./scripts/test_docker.sh --skip-stress

# Rebuild Docker images
./scripts/test_docker.sh --build
./scripts/test_vscode_docker.sh --build
./scripts/run_wsfs_docker.sh --build
```

### Debug Logging

```bash
./scripts/run_wsfs_docker.sh --debug
# Look for:
# - "Cache hit for /path/to/file"
# - "Cache miss for /path/to/file, fetching from remote"
```

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
