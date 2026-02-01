#!/bin/bash

# Docker-based Test Runner for wsfs
# Use this on Mac or when you don't have FUSE available locally
#
# Usage:
#   ./docker_test.sh [options]
#
# Options:
#   --build           Rebuild Docker image before testing
#   --fuse-only       Run only FUSE tests
#   --cache-only      Run only cache tests (with various cache configurations)
#   --no-cache-test   Skip cache configuration tests
#
# Requirements:
#   - Docker and docker-compose installed
#   - .env file with DATABRICKS_HOST and DATABRICKS_TOKEN

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

# Default options
DO_BUILD=false
FUSE_ONLY=false
CACHE_ONLY=false
NO_CACHE_TEST=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --build)
      DO_BUILD=true
      shift
      ;;
    --fuse-only)
      FUSE_ONLY=true
      shift
      ;;
    --cache-only)
      CACHE_ONLY=true
      shift
      ;;
    --no-cache-test)
      NO_CACHE_TEST=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check for .env file
if [ ! -f .env ]; then
  echo "Error: .env file not found"
  echo "Create .env with DATABRICKS_HOST and DATABRICKS_TOKEN"
  exit 1
fi

# Source .env to get credentials
set -a
source .env
set +a

if [ -z "${DATABRICKS_HOST:-}" ] || [ -z "${DATABRICKS_TOKEN:-}" ]; then
  echo "Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env"
  exit 1
fi

echo "========================================"
echo "wsfs Docker Test Runner"
echo "========================================"
echo "Databricks Host: ${DATABRICKS_HOST}"
echo ""

# Build if requested
if [ "$DO_BUILD" = true ]; then
  echo "Building Docker image..."
  docker compose build wsfs-test
  echo ""
fi

# Common docker-compose run options
DOCKER_RUN="docker compose run --rm"

# Run FUSE tests
if [ "$CACHE_ONLY" = false ]; then
  echo "========================================"
  echo "Running FUSE Tests (no cache)"
  echo "========================================"
  $DOCKER_RUN wsfs-test bash -c '
    set -e

    # Build wsfs
    go build -o /tmp/wsfs ./cmd/wsfs

    # Mount without cache
    mkdir -p /mnt/wsfs
    /tmp/wsfs --debug /mnt/wsfs &
    WSFS_PID=$!

    # Wait for mount
    for i in $(seq 1 30); do
      if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then
        break
      fi
      sleep 1
    done

    if ! grep -q " /mnt/wsfs " /proc/mounts; then
      echo "Mount failed"
      exit 1
    fi

    # Run tests
    ./scripts/tests/fuse_test.sh /mnt/wsfs

    # Cleanup
    fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
    kill $WSFS_PID 2>/dev/null || true
  '
fi

# Run cache tests
if [ "$FUSE_ONLY" = false ]; then
  echo ""
  echo "========================================"
  echo "Running Cache Tests (cache enabled)"
  echo "========================================"

  $DOCKER_RUN wsfs-test bash -c '
    set -e

    # Build wsfs
    go build -o /tmp/wsfs ./cmd/wsfs

    # Mount with cache
    mkdir -p /mnt/wsfs /tmp/wsfs-cache
    /tmp/wsfs --debug --cache=true --cache-dir=/tmp/wsfs-cache --cache-ttl=24h /mnt/wsfs > /tmp/wsfs.log 2>&1 &
    WSFS_PID=$!

    # Wait for mount
    for i in $(seq 1 30); do
      if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then
        break
      fi
      sleep 1
    done

    if ! grep -q " /mnt/wsfs " /proc/mounts; then
      echo "Mount failed"
      cat /tmp/wsfs.log
      exit 1
    fi

    # Run cache tests
    ./scripts/tests/cache_test.sh /mnt/wsfs /tmp/wsfs-cache /tmp/wsfs.log

    # Cleanup
    fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
    kill $WSFS_PID 2>/dev/null || true
  '

  # Additional cache configuration tests
  if [ "$NO_CACHE_TEST" = false ]; then
    echo ""
    echo "========================================"
    echo "Testing Cache Disabled Mode"
    echo "========================================"

    $DOCKER_RUN wsfs-test bash -c '
      set -e
      go build -o /tmp/wsfs ./cmd/wsfs

      mkdir -p /mnt/wsfs /tmp/wsfs-cache
      rm -rf /tmp/wsfs-cache/*

      /tmp/wsfs --debug --cache=false /mnt/wsfs &
      WSFS_PID=$!

      for i in $(seq 1 30); do
        if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then break; fi
        sleep 1
      done

      # Quick test - write and read
      TEST_DIR="/mnt/wsfs/cache_disabled_test_$$"
      mkdir -p "$TEST_DIR"
      echo "test content" > "$TEST_DIR/test.txt"
      CONTENT=$(cat "$TEST_DIR/test.txt")

      if [ "$CONTENT" = "test content" ]; then
        echo "✓ PASS: File operations work with cache disabled"
      else
        echo "✗ FAIL: File operations failed with cache disabled"
        exit 1
      fi

      # Verify no cache entries
      CACHE_COUNT=$(find /tmp/wsfs-cache -type f 2>/dev/null | wc -l)
      if [ "$CACHE_COUNT" -eq 0 ]; then
        echo "✓ PASS: No cache entries created when cache is disabled"
      else
        echo "✗ FAIL: Cache entries found when cache should be disabled"
        exit 1
      fi

      rm -rf "$TEST_DIR"
      fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
      kill $WSFS_PID 2>/dev/null || true
    '

    echo ""
    echo "========================================"
    echo "Testing Cache Permissions (0700/0600)"
    echo "========================================"

    $DOCKER_RUN wsfs-test bash -c '
      set -e
      go build -o /tmp/wsfs ./cmd/wsfs

      CACHE_DIR="/tmp/wsfs-cache-perm-test"
      mkdir -p /mnt/wsfs
      rm -rf "$CACHE_DIR"

      /tmp/wsfs --debug --cache=true --cache-dir="$CACHE_DIR" /mnt/wsfs &
      WSFS_PID=$!

      for i in $(seq 1 30); do
        if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then break; fi
        sleep 1
      done

      # Create a file to trigger cache
      TEST_DIR="/mnt/wsfs/perm_test_$$"
      mkdir -p "$TEST_DIR"
      echo "permission test content" > "$TEST_DIR/perm_test.txt"
      cat "$TEST_DIR/perm_test.txt" > /dev/null

      # Check cache directory permissions
      DIR_PERM=$(stat -c "%a" "$CACHE_DIR")
      if [ "$DIR_PERM" = "700" ]; then
        echo "✓ PASS: Cache directory has correct permissions (0700)"
      else
        echo "✗ FAIL: Cache directory has wrong permissions: $DIR_PERM (expected 700)"
        exit 1
      fi

      # Check cache file permissions
      CACHE_FILES=$(find "$CACHE_DIR" -type f 2>/dev/null)
      if [ -n "$CACHE_FILES" ]; then
        for f in $CACHE_FILES; do
          FILE_PERM=$(stat -c "%a" "$f")
          if [ "$FILE_PERM" = "600" ]; then
            echo "✓ PASS: Cache file has correct permissions (0600)"
          else
            echo "✗ FAIL: Cache file has wrong permissions: $FILE_PERM (expected 600)"
            exit 1
          fi
        done
      else
        echo "Note: No cache files found (may have been read from memory)"
      fi

      rm -rf "$TEST_DIR"
      fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
      kill $WSFS_PID 2>/dev/null || true
    '

    echo ""
    echo "========================================"
    echo "Testing Short TTL (5s)"
    echo "========================================"

    $DOCKER_RUN wsfs-test bash -c '
      set -e
      go build -o /tmp/wsfs ./cmd/wsfs

      mkdir -p /mnt/wsfs /tmp/wsfs-cache
      rm -rf /tmp/wsfs-cache/*

      /tmp/wsfs --debug --cache=true --cache-dir=/tmp/wsfs-cache --cache-ttl=5s /mnt/wsfs &
      WSFS_PID=$!

      for i in $(seq 1 30); do
        if grep -q " /mnt/wsfs " /proc/mounts 2>/dev/null; then break; fi
        sleep 1
      done

      TEST_DIR="/mnt/wsfs/cache_ttl_test_$$"
      mkdir -p "$TEST_DIR"
      echo "ttl test content" > "$TEST_DIR/ttl_test.txt"
      cat "$TEST_DIR/ttl_test.txt" > /dev/null

      echo "Waiting for TTL to expire (6 seconds)..."
      sleep 6

      CONTENT=$(cat "$TEST_DIR/ttl_test.txt")
      if [ "$CONTENT" = "ttl test content" ]; then
        echo "✓ PASS: File readable after cache expiry"
      else
        echo "✗ FAIL: File content incorrect after cache expiry"
        exit 1
      fi

      rm -rf "$TEST_DIR"
      fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
      kill $WSFS_PID 2>/dev/null || true
    '
  fi
fi

echo ""
echo "========================================"
echo "ALL DOCKER TESTS COMPLETED SUCCESSFULLY"
echo "========================================"
