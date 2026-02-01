#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

# Source common Docker functions
source "$ROOT_DIR/scripts/docker_common.sh"

# Build wsfs
build_wsfs

# Create mount directory
mkdir -p "$MOUNT_DIR"

# Mount wsfs (simple mode without cache options)
"$BIN_DIR/wsfs" "$MOUNT_DIR" &
WSFS_PID=$!

# Register cleanup
trap 'cleanup_wsfs "$WSFS_PID"' EXIT

# Wait for mount
if ! wait_for_mount 30; then
  exit 1
fi

# Run FUSE tests
bash "$ROOT_DIR/scripts/fuse_test.sh" "$MOUNT_DIR"

echo ""
echo "========================================"
echo "Running Large File Tests"
echo "========================================"
bash "$ROOT_DIR/scripts/large_file_test.sh" "$MOUNT_DIR"
