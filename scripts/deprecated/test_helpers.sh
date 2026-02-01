#!/bin/bash

# Common Test Helper Functions
# Source this file in test scripts: source "$(dirname "$0")/test_helpers.sh"

# Color definitions
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

# Run a command and print it
run_cmd() {
  echo -e "${YELLOW}\$ $1${NC}"
  eval "$1"
}

# Assert a condition is true
assert() {
  if eval "$1"; then
    echo -e "${GREEN}✓ PASS:${NC} $2"
  else
    echo -e "${RED}✗ FAIL:${NC} $2"
    exit 1
  fi
}

# Assert two values are equal
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

# Assert a string contains another string
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

# Cross-platform stat functions (Linux/macOS compatible)
stat_size() {
  stat -c %s "$1" 2>/dev/null || stat -f %z "$1" 2>/dev/null
}

stat_mtime() {
  stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null
}

stat_atime() {
  stat -c %X "$1" 2>/dev/null || stat -f %a "$1" 2>/dev/null
}

# Count files in a directory
count_cache_entries() {
  local cache_dir="$1"
  if [ -d "$cache_dir" ]; then
    find "$cache_dir" -type f 2>/dev/null | wc -l | tr -d ' '
  else
    echo "0"
  fi
}

# Setup test directory with cleanup trap
# Usage: setup_test_dir MOUNT_POINT TEST_NAME
# Returns: Sets TEST_BASE_DIR variable
setup_test_dir() {
  local mount_point="$1"
  local test_name="${2:-test}"

  if [ -z "$mount_point" ]; then
    echo -e "${RED}Error: Missing mount point argument.${NC}"
    exit 1
  fi

  if [ ! -d "$mount_point" ]; then
    echo -e "${RED}Error: ${mount_point} does not exist or is not a directory.${NC}"
    exit 1
  fi

  TEST_BASE_DIR_NAME="${test_name}_$(date +%s)_$$"
  TEST_BASE_DIR="${mount_point}/${TEST_BASE_DIR_NAME}"

  mkdir -p "${TEST_BASE_DIR}"
  cd "${TEST_BASE_DIR}"

  # Register cleanup on exit
  trap 'cleanup_test_dir "${TEST_BASE_DIR}"' EXIT
}

# Cleanup test directory
cleanup_test_dir() {
  local test_dir="$1"
  echo ""
  echo "========================================"
  echo "Cleanup"
  echo "========================================"
  cd /
  echo "Removing test directory: ${test_dir}"
  rm -rf "${test_dir}" 2>/dev/null || true
  echo "Cleanup complete"
}
