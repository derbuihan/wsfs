#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"

if command -v fusermount3 >/dev/null 2>&1; then
  FUSERMOUNT=fusermount3
elif command -v fusermount >/dev/null 2>&1; then
  FUSERMOUNT=fusermount
else
  echo "fusermount not found; install fuse/fuse3"
  exit 1
fi

GO_BIN=${GO_BIN:-/usr/local/go/bin/go}
MOUNT_DIR=${WSFS_MOUNT_DIR:-/mnt/wsfs}
BIN_DIR=${WSFS_BIN_DIR:-"$ROOT_DIR/tmp"}

mkdir -p "$MOUNT_DIR" "$BIN_DIR"

"$GO_BIN" build -o "$BIN_DIR/wsfs"

"$BIN_DIR/wsfs" "$MOUNT_DIR" &
WSFS_PID=$!

is_mounted() {
  grep -q " $MOUNT_DIR " /proc/mounts
}

cleanup() {
  set +e
  if is_mounted; then
    "$FUSERMOUNT" -u "$MOUNT_DIR" || umount "$MOUNT_DIR"
  fi
  kill "$WSFS_PID" 2>/dev/null || true
  wait "$WSFS_PID" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if is_mounted; then
    break
  fi
  sleep 1
done

if ! is_mounted; then
  echo "Mount did not become ready at $MOUNT_DIR"
  exit 1
fi

bash "$ROOT_DIR/scripts/fuse_test.sh" "$MOUNT_DIR"
