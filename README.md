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
- [ ] Cache files for faster access. (in progress)

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

## Testing

### Docker (recommended on macOS)

This uses `docker-compose.yml` and runs the test script (`scripts/fuse_test.sh`) inside a privileged container with FUSE enabled. The Docker image includes Vim so the save-path tests run in CI.

```bash
$ docker compose run --rm --build wsfs-test
```

Notes:
- The container reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from `.env` via `env_file`.
- `.env` is intentionally excluded from Git. Do not commit secrets.

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
