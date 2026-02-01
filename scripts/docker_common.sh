#!/usr/bin/env bash

# Common Docker Test Functions
# Source this file in Docker test scripts

set -euo pipefail

# Get the root directory of the project
ROOT_DIR="${ROOT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"

# Check required environment variables
: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"

# Detect fusermount command
detect_fusermount() {
  if command -v fusermount3 >/dev/null 2>&1; then
    echo "fusermount3"
  elif command -v fusermount >/dev/null 2>&1; then
    echo "fusermount"
  else
    echo "fusermount not found; install fuse/fuse3" >&2
    exit 1
  fi
}

FUSERMOUNT="${FUSERMOUNT:-$(detect_fusermount)}"

# Default paths
GO_BIN="${GO_BIN:-/usr/local/go/bin/go}"
MOUNT_DIR="${WSFS_MOUNT_DIR:-/mnt/wsfs}"
BIN_DIR="${WSFS_BIN_DIR:-$ROOT_DIR/tmp}"
CACHE_DIR="${WSFS_CACHE_DIR:-/tmp/wsfs-cache-test}"
LOG_FILE="${WSFS_LOG_FILE:-/tmp/wsfs-test.log}"

# Build wsfs binary
build_wsfs() {
  echo "========================================"
  echo "Building wsfs..."
  echo "========================================"
  mkdir -p "$BIN_DIR"
  "$GO_BIN" build -o "$BIN_DIR/wsfs" ./cmd/wsfs
}

# Check if filesystem is mounted
is_mounted() {
  grep -q " $MOUNT_DIR " /proc/mounts
}

# Wait for mount to be ready
wait_for_mount() {
  local timeout="${1:-30}"
  for _ in $(seq 1 "$timeout"); do
    if is_mounted; then
      return 0
    fi
    sleep 1
  done
  echo "Mount did not become ready at $MOUNT_DIR"
  return 1
}

# Unmount and cleanup
unmount_wsfs() {
  if is_mounted; then
    "$FUSERMOUNT" -u "$MOUNT_DIR" || umount "$MOUNT_DIR" || true
  fi
}

# Kill wsfs process
kill_wsfs() {
  local pid="$1"
  if [ -n "$pid" ]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}

# Full cleanup function
# Usage: cleanup_wsfs PID
cleanup_wsfs() {
  local pid="${1:-}"
  set +e
  unmount_wsfs
  kill_wsfs "$pid"
  set -e
}

# Mount wsfs with specified options
# Usage: mount_wsfs [options...]
# Returns: PID in WSFS_PID variable
mount_wsfs() {
  mkdir -p "$MOUNT_DIR"

  "$BIN_DIR/wsfs" "$@" "$MOUNT_DIR" > "$LOG_FILE" 2>&1 &
  WSFS_PID=$!

  if ! wait_for_mount 30; then
    cat "$LOG_FILE"
    exit 1
  fi

  echo "$WSFS_PID"
}

# Clean cache directory
clean_cache_dir() {
  if [ -d "$CACHE_DIR" ]; then
    rm -rf "$CACHE_DIR"/* 2>/dev/null || true
  else
    mkdir -p "$CACHE_DIR"
  fi
}
