#!/bin/bash

# Databricks CLI Verification Test Script
# Verifies that wsfs operations are correctly reflected in Databricks workspace
# by comparing with official Databricks CLI

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

# Check for Databricks CLI
if ! command -v databricks >/dev/null 2>&1; then
  echo -e "${RED}Error: databricks CLI not found.${NC}"
  echo "Please install Databricks CLI: https://docs.databricks.com/en/dev-tools/cli/index.html"
  exit 1
fi

# Check for required environment variables
: "${DATABRICKS_HOST:?DATABRICKS_HOST is required}"
: "${DATABRICKS_TOKEN:?DATABRICKS_TOKEN is required}"

TEST_BASE_DIR_NAME="databricks_cli_test_$(date +%s)_$$"
TEST_BASE_DIR="${MOUNT_POINT}/${TEST_BASE_DIR_NAME}"

echo "========================================"
echo "Databricks CLI Verification Test Suite"
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

# Get the workspace path (for Databricks CLI)
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

# ========================================
# TEST 1: File Creation and Content Verification
# ========================================
echo "========================================"
echo "TEST 1: File Creation and Content Verification"
echo "========================================"

# Create a file via wsfs
echo "Creating file via wsfs..."
TEST_CONTENT="Hello from wsfs test"
echo "$TEST_CONTENT" > test_file1.txt

# Wait for sync
sleep 2

# Verify via Databricks CLI
echo "Verifying file via Databricks CLI..."
CLI_CONTENT=$(databricks workspace export "${WORKSPACE_PATH}/test_file1.txt" --format SOURCE 2>/dev/null || echo "ERROR")

if [ "$CLI_CONTENT" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} File not found via Databricks CLI"
  exit 1
fi

# Compare content (remove trailing newline if present)
WSFS_CONTENT=$(cat test_file1.txt)
if [ "$WSFS_CONTENT" = "$CLI_CONTENT" ]; then
  echo -e "${GREEN}✓ PASS:${NC} File content matches between wsfs and Databricks CLI"
else
  echo -e "${RED}✗ FAIL:${NC} File content mismatch"
  echo -e "  wsfs content: '$WSFS_CONTENT'"
  echo -e "  CLI content:  '$CLI_CONTENT'"
  exit 1
fi

echo ""

# ========================================
# TEST 2: File Modification Verification
# ========================================
echo "========================================"
echo "TEST 2: File Modification Verification"
echo "========================================"

# Modify file via wsfs
echo "Modifying file via wsfs..."
MODIFIED_CONTENT="Modified content from wsfs"
echo "$MODIFIED_CONTENT" > test_file1.txt

# Wait for sync
sleep 2

# Verify via Databricks CLI
echo "Verifying modified content via Databricks CLI..."
CLI_MODIFIED=$(databricks workspace export "${WORKSPACE_PATH}/test_file1.txt" --format SOURCE 2>/dev/null || echo "ERROR")

if [ "$CLI_MODIFIED" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} Modified file not found via Databricks CLI"
  exit 1
fi

WSFS_MODIFIED=$(cat test_file1.txt)
if [ "$WSFS_MODIFIED" = "$CLI_MODIFIED" ]; then
  echo -e "${GREEN}✓ PASS:${NC} Modified content matches between wsfs and Databricks CLI"
else
  echo -e "${RED}✗ FAIL:${NC} Modified content mismatch"
  echo -e "  wsfs content: '$WSFS_MODIFIED'"
  echo -e "  CLI content:  '$CLI_MODIFIED'"
  exit 1
fi

echo ""

# ========================================
# TEST 3: File Rename Verification
# ========================================
echo "========================================"
echo "TEST 3: File Rename Verification"
echo "========================================"

# Create a file for rename test
echo "Creating file for rename test..."
echo "rename test content" > test_rename_old.txt
sleep 2

# Rename via wsfs
echo "Renaming file via wsfs..."
mv test_rename_old.txt test_rename_new.txt
sleep 2

# Verify old file doesn't exist via CLI
echo "Verifying old file doesn't exist via Databricks CLI..."
OLD_EXISTS=$(databricks workspace export "${WORKSPACE_PATH}/test_rename_old.txt" --format SOURCE 2>&1 || echo "NOT_FOUND")
if echo "$OLD_EXISTS" | grep -q "does not exist\|NOT_FOUND\|RESOURCE_DOES_NOT_EXIST"; then
  echo -e "${GREEN}✓ PASS:${NC} Old filename no longer exists in Databricks workspace"
else
  echo -e "${RED}✗ FAIL:${NC} Old filename still exists in Databricks workspace"
  exit 1
fi

# Verify new file exists via CLI
echo "Verifying new file exists via Databricks CLI..."
NEW_CONTENT=$(databricks workspace export "${WORKSPACE_PATH}/test_rename_new.txt" --format SOURCE 2>/dev/null || echo "ERROR")
if [ "$NEW_CONTENT" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} Renamed file not found via Databricks CLI"
  exit 1
fi

assert_eq "rename test content" "$NEW_CONTENT" "Renamed file content matches"

echo ""

# ========================================
# TEST 4: File Delete Verification
# ========================================
echo "========================================"
echo "TEST 4: File Delete Verification"
echo "========================================"

# Create a file for delete test
echo "Creating file for delete test..."
echo "delete test content" > test_delete.txt
sleep 2

# Verify file exists via CLI
echo "Verifying file exists before delete..."
BEFORE_DELETE=$(databricks workspace export "${WORKSPACE_PATH}/test_delete.txt" --format SOURCE 2>/dev/null || echo "ERROR")
if [ "$BEFORE_DELETE" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} File not found before delete"
  exit 1
fi
echo -e "${GREEN}✓${NC} File exists before delete"

# Delete via wsfs
echo "Deleting file via wsfs..."
rm test_delete.txt
sleep 2

# Verify file doesn't exist via CLI
echo "Verifying file doesn't exist after delete..."
AFTER_DELETE=$(databricks workspace export "${WORKSPACE_PATH}/test_delete.txt" --format SOURCE 2>&1 || echo "NOT_FOUND")
if echo "$AFTER_DELETE" | grep -q "does not exist\|NOT_FOUND\|RESOURCE_DOES_NOT_EXIST"; then
  echo -e "${GREEN}✓ PASS:${NC} File successfully deleted from Databricks workspace"
else
  echo -e "${RED}✗ FAIL:${NC} File still exists in Databricks workspace after delete"
  exit 1
fi

echo ""

# ========================================
# TEST 5: Directory Operations Verification
# ========================================
echo "========================================"
echo "TEST 5: Directory Operations Verification"
echo "========================================"

# Create directory via wsfs
echo "Creating directory via wsfs..."
mkdir test_dir
echo "file in directory" > test_dir/file.txt
sleep 2

# Verify directory exists via CLI
echo "Verifying directory via Databricks CLI..."
DIR_LIST=$(databricks workspace list "${WORKSPACE_PATH}/test_dir" 2>/dev/null || echo "ERROR")
if [ "$DIR_LIST" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} Directory not found via Databricks CLI"
  exit 1
fi

# Check if file exists in directory
if echo "$DIR_LIST" | grep -q "file.txt"; then
  echo -e "${GREEN}✓ PASS:${NC} Directory and file verified via Databricks CLI"
else
  echo -e "${RED}✗ FAIL:${NC} File not found in directory via Databricks CLI"
  echo "Directory listing: $DIR_LIST"
  exit 1
fi

echo ""

# ========================================
# TEST 6: Directory Rename Verification
# ========================================
echo "========================================"
echo "TEST 6: Directory Rename Verification"
echo "========================================"

# Create directory for rename test
echo "Creating directory for rename test..."
mkdir test_dir_old
echo "content in dir" > test_dir_old/file.txt
sleep 2

# Rename directory via wsfs
echo "Renaming directory via wsfs..."
mv test_dir_old test_dir_new
sleep 2

# Verify old directory doesn't exist via CLI
echo "Verifying old directory doesn't exist..."
OLD_DIR_EXISTS=$(databricks workspace list "${WORKSPACE_PATH}/test_dir_old" 2>&1 || echo "NOT_FOUND")
if echo "$OLD_DIR_EXISTS" | grep -q "does not exist\|NOT_FOUND\|RESOURCE_DOES_NOT_EXIST"; then
  echo -e "${GREEN}✓ PASS:${NC} Old directory no longer exists in Databricks workspace"
else
  echo -e "${RED}✗ FAIL:${NC} Old directory still exists in Databricks workspace"
  exit 1
fi

# Verify new directory exists via CLI
echo "Verifying new directory exists..."
NEW_DIR_LIST=$(databricks workspace list "${WORKSPACE_PATH}/test_dir_new" 2>/dev/null || echo "ERROR")
if [ "$NEW_DIR_LIST" = "ERROR" ]; then
  echo -e "${RED}✗ FAIL:${NC} Renamed directory not found via Databricks CLI"
  exit 1
fi

if echo "$NEW_DIR_LIST" | grep -q "file.txt"; then
  echo -e "${GREEN}✓ PASS:${NC} Renamed directory and content verified"
else
  echo -e "${RED}✗ FAIL:${NC} File not found in renamed directory"
  exit 1
fi

echo ""

# ========================================
# TEST 7: Directory Delete Verification
# ========================================
echo "========================================"
echo "TEST 7: Directory Delete Verification"
echo "========================================"

# Create directory for delete test
echo "Creating directory for delete test..."
mkdir test_dir_delete
echo "content" > test_dir_delete/file.txt
sleep 2

# Delete directory via wsfs
echo "Deleting directory via wsfs..."
rm -rf test_dir_delete
sleep 2

# Verify directory doesn't exist via CLI
echo "Verifying directory doesn't exist after delete..."
DELETED_DIR=$(databricks workspace list "${WORKSPACE_PATH}/test_dir_delete" 2>&1 || echo "NOT_FOUND")
if echo "$DELETED_DIR" | grep -q "does not exist\|NOT_FOUND\|RESOURCE_DOES_NOT_EXIST"; then
  echo -e "${GREEN}✓ PASS:${NC} Directory successfully deleted from Databricks workspace"
else
  echo -e "${RED}✗ FAIL:${NC} Directory still exists in Databricks workspace after delete"
  exit 1
fi

echo ""

# ========================================
# TEST 8: Large File Verification
# ========================================
echo "========================================"
echo "TEST 8: Large File Verification"
echo "========================================"

# Create a larger file (1MB)
echo "Creating 1MB file via wsfs..."
dd if=/dev/urandom of=large_file.bin bs=1M count=1 2>/dev/null

# Calculate checksum
echo "Calculating checksum..."
WSFS_CHECKSUM=$(md5sum large_file.bin | cut -d' ' -f1)

# Wait for sync
sleep 3

# Download via CLI and verify
echo "Downloading file via Databricks CLI..."
databricks workspace export "${WORKSPACE_PATH}/large_file.bin" --format AUTO --file /tmp/cli_large_file.bin 2>/dev/null

CLI_CHECKSUM=$(md5sum /tmp/cli_large_file.bin | cut -d' ' -f1)

if [ "$WSFS_CHECKSUM" = "$CLI_CHECKSUM" ]; then
  echo -e "${GREEN}✓ PASS:${NC} Large file checksum matches (wsfs and CLI)"
  echo "  Checksum: $WSFS_CHECKSUM"
else
  echo -e "${RED}✗ FAIL:${NC} Large file checksum mismatch"
  echo -e "  wsfs checksum: $WSFS_CHECKSUM"
  echo -e "  CLI checksum:  $CLI_CHECKSUM"
  exit 1
fi

# Cleanup temp file
rm -f /tmp/cli_large_file.bin

echo ""

# ========================================
# All Tests Passed
# ========================================
echo "========================================"
echo -e "${GREEN}ALL DATABRICKS CLI VERIFICATION TESTS PASSED!${NC}"
echo "========================================"
echo ""
echo -e "${BLUE}Summary:${NC}"
echo "  1. ✓ File creation and content"
echo "  2. ✓ File modification"
echo "  3. ✓ File rename"
echo "  4. ✓ File delete"
echo "  5. ✓ Directory operations"
echo "  6. ✓ Directory rename"
echo "  7. ✓ Directory delete"
echo "  8. ✓ Large file (1MB) verification"
echo ""
echo -e "${BLUE}Verification method:${NC}"
echo "  All operations performed via wsfs were verified using"
echo "  official Databricks CLI to ensure correct synchronization"
echo "  with the Databricks workspace."
echo ""
