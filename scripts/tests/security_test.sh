#!/bin/bash

# Security Test Script for wsfs Access Control
# Tests UID-based access control when --allow-other is enabled
#
# Usage: ./security_test.sh /path/to/mountpoint
#
# Requirements:
#   - Must run as root (Docker container)
#   - wsfs mounted with --allow-other flag
#   - 'nobody' user available for testing non-owner access
#
# Tests:
#   1. Owner access with --allow-other (should succeed)
#   2. Non-owner access with --allow-other (should fail with EACCES)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

# Parse arguments
MOUNT_POINT="${1:-}"
if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

# Check if running as root (required for sudo -u)
if [ "$(id -u)" -ne 0 ]; then
  echo -e "${YELLOW}Warning: Not running as root. Some tests will be skipped.${NC}"
  SKIP_NON_OWNER_TESTS=true
else
  SKIP_NON_OWNER_TESTS=false
fi

# Setup test directory
setup_test_dir "$MOUNT_POINT" "security_test"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

echo "========================================"
echo "Security Test Suite (Access Control)"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Running as: $(whoami) (UID: $(id -u))"
echo ""

# ============================================
# SECTION 1: Owner Access Tests
# ============================================
print_section "SECTION 1: Owner Access Tests"

# Test 1.1: Owner can list directory
echo "Testing: Owner can list directory..."
if ls "${TEST_BASE_DIR}" > /dev/null 2>&1; then
  echo -e "${GREEN}✓ PASS:${NC} Owner can list directory"
  ((TEST_PASSED++)) || true
else
  echo -e "${RED}✗ FAIL:${NC} Owner can list directory"
  ((TEST_FAILED++)) || true
fi

# Test 1.2: Owner can create file
TEST_FILE="${TEST_BASE_DIR}/owner_test_file.txt"
echo "Testing: Owner can create file..."
echo "test content" > "${TEST_FILE}"
assert_file_exists "${TEST_FILE}" "Owner can create file"

# Test 1.3: Owner can read file
echo "Testing: Owner can read file..."
CONTENT=$(cat "${TEST_FILE}")
assert_eq "test content" "$CONTENT" "Owner can read file content"

# Test 1.4: Owner can modify file
echo "Testing: Owner can modify file..."
echo "modified content" > "${TEST_FILE}"
CONTENT=$(cat "${TEST_FILE}")
assert_eq "modified content" "$CONTENT" "Owner can modify file"

# Test 1.5: Owner can create directory
TEST_DIR="${TEST_BASE_DIR}/owner_test_dir"
echo "Testing: Owner can create directory..."
mkdir -p "${TEST_DIR}"
assert_dir_exists "${TEST_DIR}" "Owner can create directory"

# Test 1.6: Owner can delete file
echo "Testing: Owner can delete file..."
rm "${TEST_FILE}"
assert_not_exists "${TEST_FILE}" "Owner can delete file"

# Test 1.7: Owner can delete directory
echo "Testing: Owner can delete directory..."
rmdir "${TEST_DIR}"
assert_not_exists "${TEST_DIR}" "Owner can delete directory"

# ============================================
# SECTION 2: Non-Owner Access Tests (requires root)
# ============================================
print_section "SECTION 2: Non-Owner Access Tests"

if [ "$SKIP_NON_OWNER_TESTS" = true ]; then
  skip_test "Non-owner tests require root privileges"
else
  # Create test file as owner for non-owner tests
  TEST_FILE="${TEST_BASE_DIR}/non_owner_test.txt"
  echo "test content for non-owner" > "${TEST_FILE}"

  # Test 2.1: Non-owner cannot list directory
  echo "Testing: Non-owner cannot list directory..."
  if sudo -u nobody ls "${TEST_BASE_DIR}" > /dev/null 2>&1; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to list directory"
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot list directory (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Test 2.2: Non-owner cannot read file
  echo "Testing: Non-owner cannot read file..."
  if sudo -u nobody cat "${TEST_FILE}" > /dev/null 2>&1; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to read file"
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot read file (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Test 2.3: Non-owner cannot write file
  echo "Testing: Non-owner cannot write file..."
  if sudo -u nobody sh -c "echo 'hacked' > '${TEST_FILE}'" 2>/dev/null; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to write file"
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot write file (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Test 2.4: Non-owner cannot create file
  NEW_FILE="${TEST_BASE_DIR}/non_owner_new.txt"
  echo "Testing: Non-owner cannot create file..."
  if sudo -u nobody sh -c "echo 'new file' > '${NEW_FILE}'" 2>/dev/null; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to create file"
    rm -f "${NEW_FILE}" 2>/dev/null || true
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot create file (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Test 2.5: Non-owner cannot create directory
  NEW_DIR="${TEST_BASE_DIR}/non_owner_dir"
  echo "Testing: Non-owner cannot create directory..."
  if sudo -u nobody mkdir "${NEW_DIR}" 2>/dev/null; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to create directory"
    rmdir "${NEW_DIR}" 2>/dev/null || true
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot create directory (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Test 2.6: Non-owner cannot delete file
  echo "Testing: Non-owner cannot delete file..."
  if sudo -u nobody rm "${TEST_FILE}" 2>/dev/null; then
    echo -e "${RED}✗ FAIL:${NC} Non-owner should not be able to delete file"
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} Non-owner cannot delete file (as expected)"
    ((TEST_PASSED++)) || true
  fi

  # Cleanup test file
  rm -f "${TEST_FILE}" 2>/dev/null || true
fi

# ============================================
# SECTION 3: Verify file integrity after denied access
# ============================================
print_section "SECTION 3: File Integrity Verification"

# Test 3.1: File content unchanged after denied write
TEST_FILE="${TEST_BASE_DIR}/integrity_test.txt"
ORIGINAL_CONTENT="original content $(date +%s)"
echo "${ORIGINAL_CONTENT}" > "${TEST_FILE}"

if [ "$SKIP_NON_OWNER_TESTS" = false ]; then
  # Attempt write by non-owner (should fail)
  sudo -u nobody sh -c "echo 'tampered' > '${TEST_FILE}'" 2>/dev/null || true

  CURRENT_CONTENT=$(cat "${TEST_FILE}")
  assert_eq "${ORIGINAL_CONTENT}" "${CURRENT_CONTENT}" "File content unchanged after denied write attempt"
else
  skip_test "File integrity test requires root privileges"
fi

rm -f "${TEST_FILE}" 2>/dev/null || true

# ============================================
# Test Summary
# ============================================
print_test_summary
