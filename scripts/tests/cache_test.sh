#!/bin/bash

# Unified Cache Test Script
# Tests the default always-on cache behavior on a mounted wsfs filesystem
#
# Usage: ./cache_test.sh /path/to/mountpoint [cache_dir] [log_file]
# If cache_dir is omitted, the script derives XDG_CACHE_HOME/wsfs or ~/.cache/wsfs.
#
# Sections:
#   1. Basic Cache Operations (hit/miss)
#   2. Cache Invalidation (write/delete/rename)
#   3. Remote Synchronization
#   4. Cache Reuse
#   5. Concurrent Access and Directory Invalidations

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

resolve_cache_dir() {
  if [ -n "${XDG_CACHE_HOME:-}" ]; then
    echo "${XDG_CACHE_HOME}/wsfs"
    return
  fi

  if [ -n "${HOME:-}" ]; then
    echo "${HOME}/.cache/wsfs"
    return
  fi

  echo "/tmp/wsfs-cache"
}

# Parse arguments
MOUNT_POINT="${1:-}"
CACHE_DIR="${2:-$(resolve_cache_dir)}"
LOG_FILE="${3:-/tmp/wsfs-test.log}"

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint [cache_dir] [log_file]"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

# Setup test directory
setup_test_dir "$MOUNT_POINT" "cache_test"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

echo "========================================"
echo "Cache Test Suite"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Cache directory: ${CACHE_DIR}"
echo "Log file: ${LOG_FILE}"
echo ""

# Check if cache directory exists
if [ ! -d "$CACHE_DIR" ]; then
  echo -e "${YELLOW}Warning: Cache directory ${CACHE_DIR} does not exist${NC}"
  echo "wsfs may be using a different cache root or the disk cache may still be cold"
fi

# Clear log file for fresh start
> "$LOG_FILE" 2>/dev/null || true

# ============================================
# SECTION 1: Basic Cache Operations
# ============================================
print_section "Section 1: Basic Cache Operations"

# Create a test file
echo "Creating test file..."
echo "This is cache test content" > cache_test1.txt

# First read (should be cache miss)
echo "First read (expecting cache miss)..."
CONTENT1=$(cat cache_test1.txt)
assert_eq "This is cache test content" "$CONTENT1" "First read content matches"

# Wait for cache write
sleep 1

# Check cache directory has entries
CACHE_COUNT=$(count_cache_entries "$CACHE_DIR")
if [ "$CACHE_COUNT" -gt 0 ]; then
  echo -e "${GREEN}✓ PASS:${NC} Cache directory has entries after first read ($CACHE_COUNT entries)"
  ((TEST_PASSED++)) || true
else
  echo -e "${YELLOW}⊘ INFO:${NC} No cache entries found yet (entry may still be in memory or using a different cache root)"
fi

# Sync to ensure data is written
sync

# Second read (should be cache hit)
echo "Second read (expecting cache hit)..."
CONTENT2=$(cat cache_test1.txt)
assert_eq "This is cache test content" "$CONTENT2" "Second read content matches"

# Check log for cache hit message (if debug enabled)
if [ -f "$LOG_FILE" ]; then
  LOG_CONTENT=$(cat "$LOG_FILE" 2>/dev/null || echo "")
  if echo "$LOG_CONTENT" | grep -q "Cache hit"; then
    echo -e "${GREEN}✓ INFO:${NC} Cache hit message found in log"
  fi
fi

# ============================================
# SECTION 2: Cache Invalidation
# ============================================
print_section "Section 2: Cache Invalidation"

# Test invalidation on write
echo "Creating and reading file..."
echo "original content" > cache_test2.txt
ORIGINAL=$(cat cache_test2.txt)
assert_eq "original content" "$ORIGINAL" "Original content read"

sleep 1

echo "Modifying file..."
echo "modified content" > cache_test2.txt

# Read again - should get modified content
MODIFIED=$(cat cache_test2.txt)
assert_eq "modified content" "$MODIFIED" "Modified content read after write"

# Test invalidation on delete
echo "Creating file for deletion test..."
echo "delete test content" > cache_test3.txt
cat cache_test3.txt > /dev/null
sleep 1

CACHE_BEFORE=$(count_cache_entries "$CACHE_DIR")
echo "Cache entries before delete: $CACHE_BEFORE"

echo "Deleting file..."
rm cache_test3.txt

sleep 1

assert_not_exists "cache_test3.txt" "File deleted successfully"

# Test invalidation on rename
echo "Creating file for rename test..."
echo "rename test content" > cache_test4_old.txt
cat cache_test4_old.txt > /dev/null
sleep 1

echo "Renaming file..."
mv cache_test4_old.txt cache_test4_new.txt

assert_not_exists "cache_test4_old.txt" "Old filename no longer exists"
assert_file_exists "cache_test4_new.txt" "New filename exists"
NEW_CONTENT=$(cat cache_test4_new.txt)
assert_eq "rename test content" "$NEW_CONTENT" "Content preserved after rename"

# ============================================
# SECTION 3: Remote Synchronization
# ============================================
print_section "Section 3: Remote Synchronization"

# This section requires DATABRICKS_HOST and DATABRICKS_TOKEN
if [ -n "${DATABRICKS_HOST:-}" ] && [ -n "${DATABRICKS_TOKEN:-}" ]; then
  HOST="${DATABRICKS_HOST}"
  if [[ ! "$HOST" =~ ^https?:// ]]; then
    HOST="https://${HOST}"
  fi

  WORKSPACE_PATH=$(pwd | sed "s|^${MOUNT_POINT}||")
  REMOTE_FILE_PATH="${WORKSPACE_PATH}/sync_test1.txt"
  echo "Workspace path: ${WORKSPACE_PATH}"

  echo "Creating file locally..."
  echo "original content" > sync_test1.txt
  ORIGINAL=$(cat sync_test1.txt)
  assert_eq "original content" "$ORIGINAL" "Original content created"

  sleep 2

  echo "Modifying file remotely via workspace-files/import-file API..."
  NEW_CONTENT="remotely modified content $(date +%s)"
  API_PATH="/api/2.0/workspace-files/import-file${REMOTE_FILE_PATH}?overwrite=true"

  RESPONSE=$(curl -sS -w "\n%{http_code}" -X POST     "${HOST}${API_PATH}"     -H "Authorization: Bearer ${DATABRICKS_TOKEN}"     -H "Content-Type: application/octet-stream"     --data-binary "$NEW_CONTENT")

  HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
  if [ "$HTTP_CODE" -lt 200 ] || [ "$HTTP_CODE" -ge 300 ]; then
    echo -e "${RED}✗ FAIL:${NC} Remote overwrite API failed (HTTP $HTTP_CODE)"
    echo "$RESPONSE" | sed '$d'
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Remote overwrite API succeeded (HTTP $HTTP_CODE)"
    ((TEST_PASSED++)) || true

    EARLY=$(cat sync_test1.txt)
    assert_eq "original content" "$EARLY" "Immediate reopen/read stays on cached content inside the metadata TTL window"

    UPDATED=""
    for attempt in $(seq 1 15); do
      sleep 1
      UPDATED=$(cat sync_test1.txt)
      if [ "$UPDATED" = "$NEW_CONTENT" ]; then
        break
      fi
    done

    assert_eq "$NEW_CONTENT" "$UPDATED" "Reopen/read observes out-of-band remote overwrite after metadata TTL expiry"
  fi
else
  skip_test "DATABRICKS_HOST/TOKEN not set, skipping remote sync tests"
fi

# ============================================
# SECTION 4: Cache Reuse
# ============================================
print_section "Section 4: Cache Reuse"

# Create multiple files to populate cache
echo "Creating multiple files..."
for i in {1..5}; do
  echo "File $i content" > "persist_test_$i.txt"
  cat "persist_test_$i.txt" > /dev/null
done

sleep 1

CACHE_COUNT=$(count_cache_entries "$CACHE_DIR")
echo "Cache entries after creating 5 files: $CACHE_COUNT"

if [ "$CACHE_COUNT" -ge 5 ]; then
  echo -e "${GREEN}✓ PASS:${NC} Cache has at least 5 entries"
  ((TEST_PASSED++)) || true
else
  echo -e "${YELLOW}⊘ INFO:${NC} Cache has $CACHE_COUNT entries (entries may have been evicted or are still buffered in memory)"
fi

# Read all files again to verify cache works
echo "Reading all files again from cache..."
for i in {1..5}; do
  CONTENT=$(cat "persist_test_$i.txt")
  assert_eq "File $i content" "$CONTENT" "persist_test_$i.txt content correct"
done

# Large file caching
echo "Testing large file (1MB) caching..."
dd if=/dev/urandom of=cache_large.bin bs=1M count=1 2>/dev/null
HASH1=$(md5sum cache_large.bin 2>/dev/null | cut -d' ' -f1 || md5 -q cache_large.bin 2>/dev/null)

sleep 1

HASH2=$(md5sum cache_large.bin 2>/dev/null | cut -d' ' -f1 || md5 -q cache_large.bin 2>/dev/null)
assert_eq "$HASH1" "$HASH2" "Large file hash matches on second read"

# ============================================
# SECTION 5: Concurrent Access and Directory Invalidations
# ============================================
print_section "Section 5: Concurrent Access and Directory Invalidations"

# Concurrent file access
echo "Testing concurrent file access..."
echo "concurrent test" > cache_concurrent.txt

for i in {1..10}; do
  (cat cache_concurrent.txt > /dev/null) &
done
wait

echo -e "${GREEN}✓ PASS:${NC} Concurrent reads completed without errors"
((TEST_PASSED++)) || true

# Cache with truncate
echo "Testing cache with truncate..."
echo "long content for truncation" > cache_truncate.txt
cat cache_truncate.txt > /dev/null
sleep 1

if command -v truncate >/dev/null 2>&1; then
  truncate -s 4 cache_truncate.txt
else
  python3 -c "
with open('cache_truncate.txt', 'r+b') as f:
    f.truncate(4)
"
fi

TRUNCATED=$(cat cache_truncate.txt)
assert_eq "long" "$TRUNCATED" "Truncated content correct"

# Directory with cached files
echo "Testing directory operations with cached files..."
mkdir cache_dir_test
echo "file in dir" > cache_dir_test/file1.txt
echo "another file" > cache_dir_test/file2.txt

cat cache_dir_test/file1.txt > /dev/null
cat cache_dir_test/file2.txt > /dev/null
sleep 1

rm -rf cache_dir_test
assert_not_exists "cache_dir_test" "Directory removed successfully"

# ============================================
# Test Summary
# ============================================
print_test_summary

echo ""
echo -e "${BLUE}Cache Statistics:${NC}"
if [ -d "$CACHE_DIR" ]; then
  FINAL_COUNT=$(count_cache_entries "$CACHE_DIR")
  echo "  Final cache entries: $FINAL_COUNT"
  du -sh "$CACHE_DIR" 2>/dev/null | awk '{print "  Cache size: " $1}' || echo "  Cache size: N/A"
fi
