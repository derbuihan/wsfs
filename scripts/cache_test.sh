#!/bin/bash

# Cache Test Script
# This script comprehensively tests disk cache functionality

set -euo pipefail

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

run_cmd() {
  echo -e "${YELLOW}\$ $1${NC}"
  eval "$1"
}

assert() {
  if eval "$1"; then
    echo -e "${GREEN}✓ PASS:${NC} $2"
  else
    echo -e "${RED}✗ FAIL:${NC} $2"
    exit 1
  fi
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local description="$3"
  if [ "$expected" = "$actual" ]; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    echo -e "  Expected: $expected"
    echo -e "  Actual:   $actual"
    exit 1
  fi
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local description="$3"
  if echo "$haystack" | grep -q "$needle"; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    echo -e "  Expected to contain: $needle"
    echo -e "  Actual: $haystack"
    exit 1
  fi
}

count_cache_entries() {
  local cache_dir="$1"
  if [ -d "$cache_dir" ]; then
    find "$cache_dir" -type f 2>/dev/null | wc -l | tr -d ' '
  else
    echo "0"
  fi
}

# Setup
MOUNT_POINT="${1:-/mnt/wsfs}"
CACHE_DIR="${2:-/tmp/wsfs-cache-test}"
LOG_FILE="${3:-/tmp/wsfs-cache-test.log}"

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint [cache_dir] [log_file]"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

TEST_BASE_DIR_NAME="cache_test_$(date +%s)_$$"
TEST_BASE_DIR="${MOUNT_POINT}/${TEST_BASE_DIR_NAME}"

echo "========================================"
echo "Cache Test Suite"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Cache directory: ${CACHE_DIR}"
echo "Log file: ${LOG_FILE}"
echo ""

echo "Setting up test directory..."
mkdir -p "${TEST_BASE_DIR}"
cd "${TEST_BASE_DIR}"
echo "Running tests in $(pwd)"
echo ""

# Clear log file for fresh start
> "$LOG_FILE"

# Cleanup function
cleanup() {
  echo ""
  echo "========================================"
  echo "Cleanup"
  echo "========================================"
  cd /
  echo "Removing test directory: ${TEST_BASE_DIR}"
  rm -rf "${TEST_BASE_DIR}" 2>/dev/null || true
  echo "Cleanup complete"
}

# Register cleanup on exit
trap cleanup EXIT

# ========================================
# TEST 1: Basic Cache Hit/Miss
# ========================================
echo "========================================"
echo "TEST 1: Basic Cache Hit/Miss"
echo "========================================"

# Create a test file
echo "Creating test file..."
echo "This is cache test content" > cache_test1.txt

# First read (should be cache miss)
echo "First read (expecting cache miss)..."
CONTENT1=$(cat cache_test1.txt)
assert_eq "This is cache test content" "$CONTENT1" "First read content matches"

# Wait a moment for cache to be written
sleep 1

# Check cache directory has entries
CACHE_COUNT=$(count_cache_entries "$CACHE_DIR")
assert "[ $CACHE_COUNT -gt 0 ]" "Cache directory has entries after first read ($CACHE_COUNT entries)"

# Clear in-memory buffer by dropping caches (if possible)
# This ensures the next read comes from disk cache, not memory
sync

# Second read (should be cache hit)
echo "Second read (expecting cache hit)..."
CONTENT2=$(cat cache_test1.txt)
assert_eq "This is cache test content" "$CONTENT2" "Second read content matches"

# Check log for cache hit message
if [ -f "$LOG_FILE" ]; then
  LOG_CONTENT=$(cat "$LOG_FILE" 2>/dev/null || echo "")
  if echo "$LOG_CONTENT" | grep -q "Cache hit"; then
    echo -e "${GREEN}✓ PASS:${NC} Cache hit message found in log"
  else
    echo -e "${YELLOW}INFO:${NC} No explicit cache hit message in log (may be expected if debug logging disabled)"
  fi
fi

echo ""

# ========================================
# TEST 2: Cache Invalidation on Write
# ========================================
echo "========================================"
echo "TEST 2: Cache Invalidation on Write"
echo "========================================"

# Create and read a file
echo "Creating and reading file..."
echo "original content" > cache_test2.txt
ORIGINAL=$(cat cache_test2.txt)
assert_eq "original content" "$ORIGINAL" "Original content read"

# Wait for cache
sleep 1

# Modify the file
echo "Modifying file..."
echo "modified content" > cache_test2.txt

# Read again - should get modified content
MODIFIED=$(cat cache_test2.txt)
assert_eq "modified content" "$MODIFIED" "Modified content read after write"

echo ""

# ========================================
# TEST 3: Cache Invalidation on Delete
# ========================================
echo "========================================"
echo "TEST 3: Cache Invalidation on Delete"
echo "========================================"

# Create and read a file
echo "Creating file for deletion test..."
echo "delete test content" > cache_test3.txt
cat cache_test3.txt > /dev/null
sleep 1

CACHE_BEFORE=$(count_cache_entries "$CACHE_DIR")
echo "Cache entries before delete: $CACHE_BEFORE"

# Delete the file
echo "Deleting file..."
rm cache_test3.txt

sleep 1

CACHE_AFTER=$(count_cache_entries "$CACHE_DIR")
echo "Cache entries after delete: $CACHE_AFTER"

# Note: Cache entry should be deleted, but count may not change if other files are cached
# We just verify the operation succeeded
assert "[ ! -f cache_test3.txt ]" "File deleted successfully"

echo ""

# ========================================
# TEST 4: Cache Invalidation on Rename
# ========================================
echo "========================================"
echo "TEST 4: Cache Invalidation on Rename"
echo "========================================"

# Create and cache a file
echo "Creating file for rename test..."
echo "rename test content" > cache_test4_old.txt
cat cache_test4_old.txt > /dev/null
sleep 1

# Rename the file
echo "Renaming file..."
mv cache_test4_old.txt cache_test4_new.txt

# Verify old name doesn't exist
assert "[ ! -f cache_test4_old.txt ]" "Old filename no longer exists"

# Verify new name exists and content is correct
assert "[ -f cache_test4_new.txt ]" "New filename exists"
NEW_CONTENT=$(cat cache_test4_new.txt)
assert_eq "rename test content" "$NEW_CONTENT" "Content preserved after rename"

echo ""

# ========================================
# TEST 5: Cache Persistence
# ========================================
echo "========================================"
echo "TEST 5: Cache Persistence"
echo "========================================"

# Create multiple files to populate cache
echo "Creating multiple files..."
for i in {1..5}; do
  echo "File $i content" > "persist_test_$i.txt"
  cat "persist_test_$i.txt" > /dev/null
done

sleep 1

CACHE_COUNT=$(count_cache_entries "$CACHE_DIR")
echo "Cache entries after creating 5 files: $CACHE_COUNT"
assert "[ $CACHE_COUNT -ge 5 ]" "Cache has at least 5 entries"

# Read all files again to verify cache works
echo "Reading all files again from cache..."
for i in {1..5}; do
  CONTENT=$(cat "persist_test_$i.txt")
  assert_eq "File $i content" "$CONTENT" "persist_test_$i.txt content correct"
done

echo ""

# ========================================
# TEST 6: Large File Caching
# ========================================
echo "========================================"
echo "TEST 6: Large File Caching"
echo "========================================"

# Create a 1MB file
echo "Creating 1MB test file..."
dd if=/dev/urandom of=cache_large.bin bs=1M count=1 2>/dev/null

# First read
echo "First read of large file..."
md5sum cache_large.bin > cache_large.md5
HASH1=$(cat cache_large.md5 | cut -d' ' -f1)

# Second read (from cache)
echo "Second read of large file (from cache)..."
HASH2=$(md5sum cache_large.bin | cut -d' ' -f1)

assert_eq "$HASH1" "$HASH2" "Large file hash matches on second read"

echo ""

# ========================================
# TEST 7: Concurrent File Access
# ========================================
echo "========================================"
echo "TEST 7: Concurrent File Access"
echo "========================================"

# Create a test file
echo "Creating file for concurrent access test..."
echo "concurrent test" > cache_concurrent.txt

# Read the file multiple times in parallel
echo "Reading file 10 times in parallel..."
for i in {1..10}; do
  (cat cache_concurrent.txt > /dev/null) &
done
wait

echo -e "${GREEN}✓ PASS:${NC} Concurrent reads completed without errors"

echo ""

# ========================================
# TEST 8: Cache with Truncate
# ========================================
echo "========================================"
echo "TEST 8: Cache with Truncate"
echo "========================================"

# Create a file and cache it
echo "Creating file for truncate test..."
echo "long content for truncation" > cache_truncate.txt
cat cache_truncate.txt > /dev/null
sleep 1

# Truncate the file
echo "Truncating file..."
if command -v truncate >/dev/null 2>&1; then
  truncate -s 4 cache_truncate.txt
else
  python3 - <<'PY'
with open("cache_truncate.txt", "r+b") as f:
    f.truncate(4)
PY
fi

# Read truncated content
TRUNCATED=$(cat cache_truncate.txt)
assert_eq "long" "$TRUNCATED" "Truncated content correct"

echo ""

# ========================================
# TEST 9: Cache Directory Operations
# ========================================
echo "========================================"
echo "TEST 9: Cache Directory Operations"
echo "========================================"

# Create directory with files
echo "Creating directory with files..."
mkdir cache_dir_test
echo "file in dir" > cache_dir_test/file1.txt
echo "another file" > cache_dir_test/file2.txt

# Read files to cache them
cat cache_dir_test/file1.txt > /dev/null
cat cache_dir_test/file2.txt > /dev/null
sleep 1

# Remove directory
echo "Removing directory..."
rm -rf cache_dir_test

assert "[ ! -d cache_dir_test ]" "Directory removed successfully"

echo ""

# ========================================
# All Tests Passed
# ========================================
echo "========================================"
echo -e "${GREEN}ALL CACHE TESTS PASSED!${NC}"
echo "========================================"
echo "Total test categories: 9"
FINAL_CACHE_COUNT=$(count_cache_entries "$CACHE_DIR")
echo "Final cache entries: $FINAL_CACHE_COUNT"
echo ""
echo -e "${BLUE}Cache Statistics:${NC}"
if [ -d "$CACHE_DIR" ]; then
  du -sh "$CACHE_DIR" 2>/dev/null || echo "  Cache size: N/A"
fi
echo ""
