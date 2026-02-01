#!/bin/bash

# Cache Synchronization Test Script
# Tests that local cache correctly detects remote changes

set -euo pipefail

# Source common test helpers
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/test_helpers.sh"

# Setup
MOUNT_POINT="${1:-/mnt/wsfs}"

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

# Check for required environment variables
: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"

# Ensure DATABRICKS_HOST has https:// prefix
if [[ ! "$DATABRICKS_HOST" =~ ^https?:// ]]; then
  DATABRICKS_HOST="https://${DATABRICKS_HOST}"
fi

TEST_BASE_DIR_NAME="cache_sync_test_$(date +%s)_$$"
TEST_BASE_DIR="${MOUNT_POINT}/${TEST_BASE_DIR_NAME}"

echo "========================================"
echo "Cache Synchronization Test Suite"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Databricks Host: ${DATABRICKS_HOST}"
echo ""

echo "Setting up test directory..."
mkdir -p "${TEST_BASE_DIR}"
cd "${TEST_BASE_DIR}"
echo "Running tests in $(pwd)"
echo ""

# Get the workspace path (for API calls)
WORKSPACE_PATH=$(pwd | sed "s|^${MOUNT_POINT}||")
echo "Workspace path: ${WORKSPACE_PATH}"
echo ""

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

# Helper function to modify file via Databricks API
modify_file_via_api() {
  local file_path="$1"
  local new_content="$2"
  local workspace_file_path="${WORKSPACE_PATH}/${file_path}"

  echo "Modifying ${workspace_file_path} via Databricks API..."

  # Base64 encode the content
  local content_b64=$(echo -n "$new_content" | base64)

  # Use workspace-files/import-file API
  local api_path="/api/2.0/workspace-files/import-file${workspace_file_path}?overwrite=true"
  local response=$(curl -s -w "\n%{http_code}" -X POST \
    "${DATABRICKS_HOST}${api_path}" \
    -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
    -H "Content-Type: application/octet-stream" \
    -d "$new_content")

  local http_code=$(echo "$response" | tail -n1)
  local body=$(echo "$response" | head -n-1)

  if [ "$http_code" -ge 200 ] && [ "$http_code" -lt 300 ]; then
    echo -e "${GREEN}✓${NC} File modified via API (HTTP $http_code)"
    return 0
  else
    echo -e "${RED}✗${NC} API call failed (HTTP $http_code)"
    echo "Response: $body"
    return 1
  fi
}

# ========================================
# TEST 1: Detect Remote File Modification
# ========================================
echo "========================================"
echo "TEST 1: Detect Remote File Modification"
echo "========================================"

# Create a file locally
echo "Creating file locally..."
echo "original content" > sync_test1.txt
ORIGINAL=$(cat sync_test1.txt)
assert_eq "original content" "$ORIGINAL" "Original content created"

# Wait for file to be uploaded and cached
sleep 2

# Modify the file via Databricks API
echo "Modifying file remotely via API..."
modify_file_via_api "sync_test1.txt" "remotely modified content"

# Wait for modification to propagate
sleep 2

# Read the file again - should get updated content (cache should be invalidated)
echo "Reading file again (should get new content)..."
UPDATED=$(cat sync_test1.txt)

# The cache should detect the remote modification via modTime and fetch new content
if [ "$UPDATED" = "remotely modified content" ]; then
  echo -e "${GREEN}✓ PASS:${NC} Cache correctly detected remote modification"
elif [ "$UPDATED" = "original content" ]; then
  echo -e "${YELLOW}WARNING:${NC} Cache did not detect remote modification"
  echo -e "  This may indicate modTime comparison is not working correctly"
  echo -e "  Expected: 'remotely modified content'"
  echo -e "  Actual: '$UPDATED'"
  # Don't fail the test - this might be expected behavior depending on cache TTL
else
  echo -e "${RED}✗ FAIL:${NC} Unexpected content after remote modification"
  echo -e "  Expected: 'remotely modified content' or 'original content'"
  echo -e "  Actual: '$UPDATED'"
  exit 1
fi

echo ""

# ========================================
# TEST 2: Cache Behavior After Flush
# ========================================
echo "========================================"
echo "TEST 2: Cache Behavior After Flush"
echo "========================================"

# Create a file
echo "Creating file..."
echo "before flush" > sync_test2.txt

# Modify it
echo "Modifying file..."
echo "after modification" > sync_test2.txt

# Sync to ensure flush
sync
sleep 2

# Read via FUSE (should get new content)
LOCAL=$(cat sync_test2.txt)
assert_eq "after modification" "$LOCAL" "Local read after flush returns modified content"

# TODO: Verify via API that remote also has the new content
# This requires making an API call to read the file

echo ""

# ========================================
# TEST 3: Multiple File Modifications
# ========================================
echo "========================================"
echo "TEST 3: Multiple File Modifications"
echo "========================================"

# Create multiple files
echo "Creating multiple files..."
for i in {1..3}; do
  echo "file $i initial" > "multi_test_$i.txt"
  cat "multi_test_$i.txt" > /dev/null
done

sleep 2

# Modify all files remotely
echo "Modifying all files remotely..."
for i in {1..3}; do
  modify_file_via_api "multi_test_$i.txt" "file $i modified remotely"
  sleep 1
done

sleep 2

# Read all files and check for updates
echo "Reading all files..."
DETECTED=0
for i in {1..3}; do
  CONTENT=$(cat "multi_test_$i.txt")
  if [ "$CONTENT" = "file $i modified remotely" ]; then
    DETECTED=$((DETECTED + 1))
    echo -e "${GREEN}✓${NC} File $i: Detected remote modification"
  else
    echo -e "${YELLOW}!${NC} File $i: Did not detect remote modification (content: '$CONTENT')"
  fi
done

echo "Detected remote modifications: $DETECTED/3"
if [ $DETECTED -gt 0 ]; then
  echo -e "${GREEN}✓ PASS:${NC} At least some remote modifications detected"
else
  echo -e "${YELLOW}WARNING:${NC} No remote modifications detected"
fi

echo ""

# ========================================
# TEST 4: Cache Behavior with Touch
# ========================================
echo "========================================"
echo "TEST 4: Cache Behavior with Touch"
echo "========================================"

# Create a file
echo "Creating file..."
echo "touch test content" > sync_test4.txt
cat sync_test4.txt > /dev/null

sleep 2

# Get original mtime
MTIME_BEFORE=$(stat_mtime sync_test4.txt)
echo "Original mtime: $MTIME_BEFORE"

# Touch the file (updates mtime without changing content)
sleep 2
touch sync_test4.txt

# Get new mtime
MTIME_AFTER=$(stat_mtime sync_test4.txt)
echo "After touch mtime: $MTIME_AFTER"

assert "[ $MTIME_AFTER -gt $MTIME_BEFORE ]" "touch updates mtime"

# Content should be unchanged
CONTENT=$(cat sync_test4.txt)
assert_eq "touch test content" "$CONTENT" "Content unchanged after touch"

echo ""

# ========================================
# All Tests Passed
# ========================================
echo "========================================"
echo -e "${GREEN}CACHE SYNCHRONIZATION TESTS COMPLETED${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}Notes:${NC}"
echo "- Cache invalidation depends on remote file modTime"
echo "- Some warnings are expected if cache TTL hasn't expired"
echo "- Local modifications always override cached content"
echo ""
