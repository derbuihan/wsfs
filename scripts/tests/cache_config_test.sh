#!/bin/bash
#
# Cache Configuration Tests for wsfs
# Tests different cache configurations by mounting/unmounting with different options
#
# Usage: ./cache_config_test.sh WSFS_BINARY MOUNT_POINT [CACHE_DIR]
#
# Example:
#   ./cache_config_test.sh /tmp/wsfs /mnt/wsfs
#   ./cache_config_test.sh ./wsfs /mnt/wsfs /tmp/my-cache

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

# ============================================
# Configuration
# ============================================

WSFS_BINARY=""
MOUNT_POINT=""
CACHE_DIR="/tmp/wsfs-cache"
MOUNT_TIMEOUT=30

# ============================================
# Helper Functions
# ============================================

mount_wsfs() {
  local opts="$1"

  echo "Mounting wsfs with options: $opts"

  # Clean up any existing mount
  unmount_wsfs "$MOUNT_POINT"

  # Create mount point if needed
  mkdir -p "$MOUNT_POINT"

  # Mount
  $WSFS_BINARY $opts "$MOUNT_POINT" > /tmp/wsfs-config-test.log 2>&1 &
  WSFS_PID=$!

  # Wait for mount
  if ! wait_for_mount "$MOUNT_POINT" "$MOUNT_TIMEOUT"; then
    echo -e "${RED}Mount failed. Log:${NC}"
    cat /tmp/wsfs-config-test.log
    return 1
  fi

  echo "wsfs mounted (PID: $WSFS_PID)"
  return 0
}

cleanup_mount() {
  echo "Cleaning up mount..."
  unmount_wsfs "$MOUNT_POINT"
  if [ -n "${WSFS_PID:-}" ]; then
    kill "$WSFS_PID" 2>/dev/null || true
  fi
}

# ============================================
# Main
# ============================================

main() {
  if [ $# -lt 2 ]; then
    echo "Usage: $0 WSFS_BINARY MOUNT_POINT [CACHE_DIR]"
    echo "Example: $0 /tmp/wsfs /mnt/wsfs"
    exit 1
  fi

  WSFS_BINARY="$1"
  MOUNT_POINT="$2"
  CACHE_DIR="${3:-/tmp/wsfs-cache}"

  if [ ! -x "$WSFS_BINARY" ]; then
    echo -e "${RED}Error: $WSFS_BINARY is not executable${NC}"
    exit 1
  fi

  print_section "wsfs Cache Configuration Tests"
  echo "wsfs binary: $WSFS_BINARY"
  echo "Mount point: $MOUNT_POINT"
  echo "Cache directory: $CACHE_DIR"

  # Ensure cleanup on exit
  trap cleanup_mount EXIT

  # Run tests
  test_cache_disabled
  test_cache_permissions
  test_short_ttl

  print_test_summary
}

# ============================================
# Test 1: Cache Disabled Mode
# ============================================

test_cache_disabled() {
  print_section "Test 1: Cache Disabled Mode"

  # Clean cache directory
  rm -rf "${CACHE_DIR:?}"/* 2>/dev/null || true
  mkdir -p "$CACHE_DIR"

  if ! mount_wsfs "--debug --cache=false"; then
    echo -e "${RED}FAIL:${NC} Failed to mount with cache disabled"
    ((TEST_FAILED++)) || true
    return
  fi

  # Create test directory and file
  local test_dir="$MOUNT_POINT/cache_disabled_test_$$"
  mkdir -p "$test_dir"
  echo "test content" > "$test_dir/test.txt"

  # Read back and verify
  local content
  content=$(cat "$test_dir/test.txt" 2>/dev/null || echo "ERROR")

  if [ "$content" = "test content" ]; then
    echo -e "${GREEN}PASS:${NC} File operations work with cache disabled"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} File operations failed with cache disabled"
    ((TEST_FAILED++)) || true
  fi

  # Verify no cache entries created
  local cache_count
  cache_count=$(find "$CACHE_DIR" -type f 2>/dev/null | wc -l | tr -d ' ')

  if [ "$cache_count" -eq 0 ]; then
    echo -e "${GREEN}PASS:${NC} No cache entries created when cache is disabled"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} Cache entries found when cache should be disabled ($cache_count files)"
    ((TEST_FAILED++)) || true
  fi

  # Cleanup
  rm -rf "$test_dir" 2>/dev/null || true
  cleanup_mount
}

# ============================================
# Test 2: Cache Permissions
# ============================================

test_cache_permissions() {
  print_section "Test 2: Cache Permissions (0700/0600)"

  # Use a separate cache directory for this test
  local perm_cache_dir="/tmp/wsfs-cache-perm-test"
  rm -rf "$perm_cache_dir"

  if ! mount_wsfs "--debug --cache=true --cache-dir=$perm_cache_dir"; then
    echo -e "${RED}FAIL:${NC} Failed to mount for permissions test"
    ((TEST_FAILED++)) || true
    return
  fi

  # Create a file to trigger cache
  local test_dir="$MOUNT_POINT/perm_test_$$"
  mkdir -p "$test_dir"
  echo "permission test content" > "$test_dir/perm_test.txt"
  cat "$test_dir/perm_test.txt" > /dev/null

  # Check cache directory permissions
  if [ -d "$perm_cache_dir" ]; then
    local dir_perm
    if is_linux; then
      dir_perm=$(stat -c "%a" "$perm_cache_dir")
    else
      dir_perm=$(stat -f "%Lp" "$perm_cache_dir")
    fi

    if [ "$dir_perm" = "700" ]; then
      echo -e "${GREEN}PASS:${NC} Cache directory has correct permissions (0700)"
      ((TEST_PASSED++)) || true
    else
      echo -e "${RED}FAIL:${NC} Cache directory has wrong permissions: $dir_perm (expected 700)"
      ((TEST_FAILED++)) || true
    fi
  else
    echo -e "${YELLOW}SKIP:${NC} Cache directory not created"
    ((TEST_SKIPPED++)) || true
  fi

  # Check cache file permissions
  local cache_files
  cache_files=$(find "$perm_cache_dir" -type f 2>/dev/null)

  if [ -n "$cache_files" ]; then
    local all_correct=true
    for f in $cache_files; do
      local file_perm
      if is_linux; then
        file_perm=$(stat -c "%a" "$f")
      else
        file_perm=$(stat -f "%Lp" "$f")
      fi

      if [ "$file_perm" != "600" ]; then
        echo -e "${RED}FAIL:${NC} Cache file has wrong permissions: $file_perm (expected 600)"
        all_correct=false
        break
      fi
    done

    if [ "$all_correct" = true ]; then
      echo -e "${GREEN}PASS:${NC} Cache files have correct permissions (0600)"
      ((TEST_PASSED++)) || true
    else
      ((TEST_FAILED++)) || true
    fi
  else
    echo -e "${YELLOW}NOTE:${NC} No cache files found (may have been read from memory)"
  fi

  # Cleanup
  rm -rf "$test_dir" 2>/dev/null || true
  cleanup_mount
  rm -rf "$perm_cache_dir" 2>/dev/null || true
}

# ============================================
# Test 3: Short TTL
# ============================================

test_short_ttl() {
  print_section "Test 3: Short TTL (5s)"

  # Clean cache directory
  rm -rf "${CACHE_DIR:?}"/* 2>/dev/null || true
  mkdir -p "$CACHE_DIR"

  if ! mount_wsfs "--debug --cache=true --cache-dir=$CACHE_DIR --cache-ttl=5s"; then
    echo -e "${RED}FAIL:${NC} Failed to mount with short TTL"
    ((TEST_FAILED++)) || true
    return
  fi

  # Create test file
  local test_dir="$MOUNT_POINT/cache_ttl_test_$$"
  mkdir -p "$test_dir"
  echo "ttl test content" > "$test_dir/ttl_test.txt"

  # Read to populate cache
  cat "$test_dir/ttl_test.txt" > /dev/null

  echo "Waiting for TTL to expire (6 seconds)..."
  sleep 6

  # Read again after TTL expiry
  local content
  content=$(cat "$test_dir/ttl_test.txt" 2>/dev/null || echo "ERROR")

  if [ "$content" = "ttl test content" ]; then
    echo -e "${GREEN}PASS:${NC} File readable after cache expiry"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} File content incorrect after cache expiry"
    ((TEST_FAILED++)) || true
  fi

  # Cleanup
  rm -rf "$test_dir" 2>/dev/null || true
  cleanup_mount
}

# ============================================
# Run Main
# ============================================

main "$@"
