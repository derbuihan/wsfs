# AGENTS.md

AI coding agent guidance for this repo.

## Workflow (must follow)
1. Confirm `Task.md` and select the task to work on.
2. Implement the task.
3. Run tests to confirm no regression.
   - Always run `scripts/tests/fuse_test.sh` after implementation. If it fails, fix the failing part.
4. Update `Task.md` when progress changes.

## Project overview
`wsfs` is a FUSE-based filesystem that mounts Databricks Workspace Files locally.

Key entry points:
- `main.go`: CLI entry point and mount setup.
- `client.go`: Databricks Workspace Files client.
- `node.go`: FUSE node implementation.
- `scripts/fuse_test.sh`: Filesystem integration tests.

## Current behavior notes
- `Setattr` supports size changes (truncate) and mtime updates; mode/uid/gid are ENOTSUP.
- atime-only updates are ENOTSUP; combined mtime+atime (e.g., `touch`) works.
- Stable inode IDs are derived from Databricks `ObjectId`/`ResourceId`/`Path` to avoid editor save errors.
- Vim save paths (default/backup/swap) are validated in `scripts/fuse_test.sh`.

## Environment
Required env vars (do not commit secrets):
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

Local `.env` is expected for development, but must never be committed.

## Build & run
Build:
```bash
go build -o wsfs
```

Run:
```bash
./wsfs <mount-point>
```

Debug:
```bash
./wsfs -debug <mount-point>
```

## Tests
Docker (macOS recommended):
```bash
docker compose run --rm --build wsfs-test
```

Debugging failing shell tests (Docker):
- If a shell test fails, re-run that script directly in a Docker container to isolate the failure.
```bash
docker compose run --rm wsfs-test bash -c '
  set -e
  go build -o /tmp/wsfs ./cmd/wsfs
  mkdir -p /mnt/wsfs /tmp/wsfs-cache
  /tmp/wsfs --debug --cache=true --cache-dir=/tmp/wsfs-cache --cache-ttl=24h /mnt/wsfs > /tmp/wsfs.log 2>&1 &
  WSFS_PID=$!
  for i in $(seq 1 30); do
    if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then break; fi
    sleep 1
  done
  ./scripts/tests/fuse_test.sh /mnt/wsfs
  TEST_RESULT=$?
  fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
  kill $WSFS_PID 2>/dev/null || true
  exit $TEST_RESULT
'
```
- When needed, replace `./scripts/tests/fuse_test.sh /mnt/wsfs` with a smaller failing script or a minimal inline repro to narrow down the root cause.

Linux (direct):
```bash
sudo apt-get update
sudo apt-get install -y fuse3
sudo apt-get install -y vim
echo 'user_allow_other' | sudo tee -a /etc/fuse.conf
mkdir -p /mnt/wsfs
go build -o tmp/wsfs
./tmp/wsfs /mnt/wsfs &
./scripts/tests/fuse_test.sh /mnt/wsfs
fusermount3 -u /mnt/wsfs
```

## Coding conventions
- Keep changes small and focused.
- Avoid logging noise; gate diagnostics behind `-debug`.
- Prefer clear, maintainable Go code over cleverness.

## Repository hygiene
- Do not commit secrets (e.g., `.env`).
- Update README when adding new workflows or commands.
