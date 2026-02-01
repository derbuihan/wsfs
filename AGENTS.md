# AGENTS.md

This file describes how to work on this repository as an AI coding agent.

## Project overview

`wsfs` is a FUSE-based filesystem that mounts Databricks Workspace Files into a local path.

Key entry points:
- `main.go`: CLI entry point and mount setup.
- `client.go`: Databricks Workspace Files client.
- `node.go`: FUSE node implementation.
- `scripts/fuse_test.sh`: Filesystem integration tests.

## Current implementation notes

- `Setattr` supports size changes (truncate) and mtime updates; mode/uid/gid are explicitly unsupported (return ENOTSUP). atime-only updates are unsupported; combined mtime+atime (e.g., `touch`) works.
- Stable inode IDs are derived from Databricks `ObjectId`/`ResourceId`/`Path` to avoid editor save errors (e.g., Vim E949).
- Vim save paths (default/backup/swap) are validated in `scripts/fuse_test.sh`.

## Environment

Required environment variables (do not commit secrets):
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

The repository expects a local `.env` for development, but `.env` must never be committed.

## Build

```bash
go build -o wsfs
```

## Run (local)

```bash
./wsfs <mount-point>
```

Use `-debug` to enable verbose FUSE logs:

```bash
./wsfs -debug <mount-point>
```

## Tests

### Docker (recommended on macOS)

Uses `docker-compose.yml`, mounts `/dev/fuse`, and runs the existing test script:

```bash
docker compose run --rm --build wsfs-test
```

Notes:
- Development is commonly done on macOS using Docker; the test image includes Vim to exercise editor save behavior.

### Linux (direct)

```bash
sudo apt-get update
sudo apt-get install -y fuse3
echo 'user_allow_other' | sudo tee -a /etc/fuse.conf
mkdir -p /mnt/wsfs
go build -o tmp/wsfs
./tmp/wsfs /mnt/wsfs &
./scripts/fuse_test.sh /mnt/wsfs
fusermount3 -u /mnt/wsfs
```

## Coding conventions

- Keep changes small and focused.
- Avoid logging noise; use `-debug` gated logs when adding diagnostics.
- Prefer clear, maintainable Go code over cleverness.

## Repository hygiene

- Do not commit secrets (e.g., `.env`).
- Prefer updating README when adding new workflows or commands.
