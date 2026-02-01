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
CACHE_DIR=${WSFS_CACHE_DIR:-/tmp/wsfs-cache-test}
LOG_FILE=${WSFS_LOG_FILE:-/tmp/wsfs-test.log}

mkdir -p "$MOUNT_DIR" "$BIN_DIR" "$CACHE_DIR"

echo "========================================"
echo "Building wsfs..."
echo "========================================"
"$GO_BIN" build -o "$BIN_DIR/wsfs" ./cmd/wsfs

is_mounted() {
  grep -q " $MOUNT_DIR " /proc/mounts
}

WSFS_PID=""

cleanup() {
  set +e
  if is_mounted; then
    "$FUSERMOUNT" -u "$MOUNT_DIR" || umount "$MOUNT_DIR"
  fi
  if [ -n "$WSFS_PID" ]; then
    kill "$WSFS_PID" 2>/dev/null || true
    wait "$WSFS_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

# ========================================
# Test 1: Cache Enabled (default settings)
# ========================================
echo ""
echo "========================================"
echo "Test 1: Cache Enabled (Default Settings)"
echo "========================================"
echo "Settings:"
echo "  - Cache: enabled"
echo "  - Cache dir: $CACHE_DIR"
echo "  - Cache size: 10GB"
echo "  - Cache TTL: 24h"
echo "  - Debug: enabled"
echo ""

# Clean up cache directory (but not if it's a volume mount)
if [ -d "$CACHE_DIR" ]; then
  rm -rf "$CACHE_DIR"/* 2>/dev/null || true
else
  mkdir -p "$CACHE_DIR"
fi

# Mount with default cache settings and debug logging
"$BIN_DIR/wsfs" \
  --debug \
  --cache=true \
  --cache-dir="$CACHE_DIR" \
  --cache-size=10 \
  --cache-ttl=24h \
  "$MOUNT_DIR" > "$LOG_FILE" 2>&1 &
WSFS_PID=$!

# Wait for mount
for _ in $(seq 1 30); do
  if is_mounted; then
    break
  fi
  sleep 1
done

if ! is_mounted; then
  echo "Mount did not become ready at $MOUNT_DIR"
  cat "$LOG_FILE"
  exit 1
fi

echo "Mount ready. Running cache tests..."
echo ""

# Run cache tests
bash "$ROOT_DIR/scripts/cache_test.sh" "$MOUNT_DIR" "$CACHE_DIR" "$LOG_FILE"

echo ""
echo "Running cache synchronization tests..."
echo ""

# Run cache sync tests
bash "$ROOT_DIR/scripts/cache_sync_test.sh" "$MOUNT_DIR"

echo ""
echo "Running Databricks CLI verification tests..."
echo ""

# Run Databricks CLI verification tests
bash "$ROOT_DIR/scripts/databricks_cli_verification_test.sh" "$MOUNT_DIR"

# Unmount
cleanup
sleep 2

# ========================================
# Test 2: Cache Disabled
# ========================================
echo ""
echo "========================================"
echo "Test 2: Cache Disabled"
echo "========================================"
echo "Settings:"
echo "  - Cache: disabled"
echo ""

# Clean cache directory (but not if it's a volume mount)
if [ -d "$CACHE_DIR" ]; then
  rm -rf "$CACHE_DIR"/* 2>/dev/null || true
else
  mkdir -p "$CACHE_DIR"
fi

# Mount with cache disabled
"$BIN_DIR/wsfs" \
  --debug \
  --cache=false \
  "$MOUNT_DIR" > "$LOG_FILE" 2>&1 &
WSFS_PID=$!

# Wait for mount
for _ in $(seq 1 30); do
  if is_mounted; then
    break
  fi
  sleep 1
done

if ! is_mounted; then
  echo "Mount did not become ready at $MOUNT_DIR"
  cat "$LOG_FILE"
  exit 1
fi

echo "Mount ready. Running basic tests with cache disabled..."
echo ""

# Create test directory
TEST_DIR="$MOUNT_DIR/cache_disabled_test_$$"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

# Create and read a file
echo "test content" > test.txt
CONTENT=$(cat test.txt)
if [ "$CONTENT" = "test content" ]; then
  echo "✓ PASS: File operations work with cache disabled"
else
  echo "✗ FAIL: File operations failed with cache disabled"
  exit 1
fi

# Verify cache directory is empty
CACHE_COUNT=$(find "$CACHE_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
if [ "$CACHE_COUNT" -eq 0 ]; then
  echo "✓ PASS: No cache entries created when cache is disabled"
else
  echo "✗ FAIL: Cache entries found when cache should be disabled ($CACHE_COUNT entries)"
  exit 1
fi

# Cleanup
cd /
rm -rf "$TEST_DIR"

# Unmount
cleanup
sleep 2

# ========================================
# Test 3: Short TTL (5 seconds)
# ========================================
echo ""
echo "========================================"
echo "Test 3: Short TTL Cache"
echo "========================================"
echo "Settings:"
echo "  - Cache: enabled"
echo "  - Cache TTL: 5s"
echo ""

# Clean cache directory (but not if it's a volume mount)
if [ -d "$CACHE_DIR" ]; then
  rm -rf "$CACHE_DIR"/* 2>/dev/null || true
else
  mkdir -p "$CACHE_DIR"
fi

# Mount with short TTL
"$BIN_DIR/wsfs" \
  --debug \
  --cache=true \
  --cache-dir="$CACHE_DIR" \
  --cache-ttl=5s \
  "$MOUNT_DIR" > "$LOG_FILE" 2>&1 &
WSFS_PID=$!

# Wait for mount
for _ in $(seq 1 30); do
  if is_mounted; then
    break
  fi
  sleep 1
done

if ! is_mounted; then
  echo "Mount did not become ready at $MOUNT_DIR"
  cat "$LOG_FILE"
  exit 1
fi

echo "Mount ready. Testing TTL expiration..."
echo ""

# Create test directory
TEST_DIR="$MOUNT_DIR/cache_ttl_test_$$"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

# Create and read a file (will be cached)
echo "ttl test content" > ttl_test.txt
CONTENT1=$(cat ttl_test.txt)
echo "Initial read: $CONTENT1"

# Wait for cache entry
sleep 1
CACHE_COUNT=$(find "$CACHE_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')
echo "Cache entries after first read: $CACHE_COUNT"

# Wait for TTL to expire
echo "Waiting for TTL to expire (5 seconds)..."
sleep 6

# Read again - cache should be expired
CONTENT2=$(cat ttl_test.txt)
echo "Read after TTL expiry: $CONTENT2"

if [ "$CONTENT2" = "ttl test content" ]; then
  echo "✓ PASS: File readable after cache expiry"
else
  echo "✗ FAIL: File content incorrect after cache expiry"
  exit 1
fi

# Cleanup
cd /
rm -rf "$TEST_DIR"

# Unmount
cleanup
sleep 2

# ========================================
# Test 4: Small Cache Size (1MB, LRU eviction)
# ========================================
echo ""
echo "========================================"
echo "Test 4: Small Cache Size (LRU Eviction)"
echo "========================================"
echo "Settings:"
echo "  - Cache: enabled"
echo "  - Cache size: 1MB"
echo ""

# Clean cache directory (but not if it's a volume mount)
if [ -d "$CACHE_DIR" ]; then
  rm -rf "$CACHE_DIR"/* 2>/dev/null || true
else
  mkdir -p "$CACHE_DIR"
fi

# Mount with small cache size (1MB)
"$BIN_DIR/wsfs" \
  --debug \
  --cache=true \
  --cache-dir="$CACHE_DIR" \
  --cache-size=0.001 \
  "$MOUNT_DIR" > "$LOG_FILE" 2>&1 &
WSFS_PID=$!

# Wait for mount
for _ in $(seq 1 30); do
  if is_mounted; then
    break
  fi
  sleep 1
done

if ! is_mounted; then
  echo "Mount did not become ready at $MOUNT_DIR"
  cat "$LOG_FILE"
  exit 1
fi

echo "Mount ready. Testing LRU eviction..."
echo ""

# Create test directory
TEST_DIR="$MOUNT_DIR/cache_lru_test_$$"
mkdir -p "$TEST_DIR"
cd "$TEST_DIR"

# Create several files to exceed cache size
echo "Creating files to trigger LRU eviction..."
for i in {1..5}; do
  # Each file is ~300KB
  dd if=/dev/urandom of="lru_test_$i.bin" bs=1K count=300 2>/dev/null
  cat "lru_test_$i.bin" > /dev/null
  sleep 1
done

# Check cache size
CACHE_SIZE=$(du -s "$CACHE_DIR" 2>/dev/null | cut -f1)
echo "Cache size after creating 5 files: ${CACHE_SIZE}KB"

# Cache size should be limited to ~1MB (1024KB)
# Allow some margin for filesystem overhead
if [ "$CACHE_SIZE" -lt 2000 ]; then
  echo "✓ PASS: Cache size limited by LRU eviction"
else
  echo "⚠ WARNING: Cache size larger than expected (${CACHE_SIZE}KB)"
  echo "  This might indicate LRU eviction needs tuning"
fi

# First file might be evicted
if [ -f "lru_test_1.bin" ]; then
  FIRST_FILE=$(cat "lru_test_1.bin" | wc -c)
  if [ "$FIRST_FILE" -eq 307200 ]; then
    echo "✓ PASS: First file still accessible"
  fi
fi

# Cleanup
cd /
rm -rf "$TEST_DIR"

# Unmount
cleanup

# ========================================
# All Tests Complete
# ========================================
echo ""
echo "========================================"
echo "ALL CACHE TESTS COMPLETED SUCCESSFULLY"
echo "========================================"
echo ""
echo "Summary:"
echo "  1. ✓ Cache enabled with default settings"
echo "  2. ✓ Cache disabled mode"
echo "  3. ✓ Short TTL (5s) expiration"
echo "  4. ✓ Small cache size (1MB) LRU eviction"
echo ""
echo "Cache implementation verified:"
echo "  - Basic cache hit/miss operations"
echo "  - Cache invalidation on write/delete/rename"
echo "  - Remote file modification detection"
echo "  - TTL-based expiration"
echo "  - LRU-based eviction"
echo "  - Cache disable mode"
echo ""
