# AGENTS.md

AI coding agent guidance for this repo.

## Workflow (must follow)
1. Confirm `Task.md` and select the task to work on.
2. Implement the task.
3. Run tests to confirm no regression.
   - Always run the FUSE test suite after implementation (normally `./scripts/run_tests_docker.sh --fuse-only`). If it fails, fix the failing part.
4. Update `Task.md` when progress changes.

## Project overview
`wsfs` is a FUSE-based filesystem that mounts Databricks Workspace Files locally.

Key entry points:
- `main.go`: CLI entry point and mount setup.
- `client.go`: Databricks Workspace Files client.
- `node.go`: FUSE node implementation.
- `scripts/fuse_test.sh`: Filesystem integration tests.

## Current behavior notes
- `Setattr` supports size changes (truncate); timestamp-only updates on existing files return `ENOTSUP`.
- mode/uid/gid changes are `ENOTSUP`.
- Stable inode IDs are derived from Databricks `ObjectId`/`ResourceId`/`Path` to avoid editor save errors.
- Vim save paths (default/backup/swap) are validated in `scripts/fuse_test.sh`.

## Environment
Required env vars (do not commit secrets):
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`

Local `.env` is expected for development, but must never be committed.

## Build & run
Use the Docker shell wrapper for source-checkout development on both macOS and Linux.
The wsfs mount lives inside the container shell, not on the host filesystem.

Open an interactive shell with wsfs mounted at `/mnt/wsfs`:
```bash
./scripts/run_wsfs_docker.sh
```

Open the same shell with debug logging enabled:
```bash
./scripts/run_wsfs_docker.sh --debug
```

Run one command inside the mounted container instead of an interactive shell:
```bash
./scripts/run_wsfs_docker.sh -- 'ls -la /mnt/wsfs'
```

## Tests
Docker (recommended on macOS and Linux):
```bash
./scripts/run_tests_docker.sh
```

Debugging failing shell tests (Docker):
- If a shell test fails, re-run that script directly in a Docker container to isolate the failure.
```bash
docker compose run --rm wsfs-test bash -c '
  set -e
  go build -o /tmp/wsfs ./cmd/wsfs
  export XDG_CACHE_HOME=/tmp/xdg-cache
  mkdir -p /mnt/wsfs "$XDG_CACHE_HOME/wsfs"
  /tmp/wsfs --debug /mnt/wsfs > /tmp/wsfs.log 2>&1 &
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


## Coding conventions
- Keep changes small and focused.
- Avoid logging noise; gate diagnostics behind `-debug`.
- Prefer clear, maintainable Go code over cleverness.

## Repository hygiene
- Do not commit secrets (e.g., `.env`).
- Update README when adding new workflows or commands.
