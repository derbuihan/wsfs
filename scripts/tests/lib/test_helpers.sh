#!/bin/bash

# Common Test Helper Functions for wsfs
# Source this file in test scripts: source "$(dirname "$0")/lib/test_helpers.sh"
#
# This library provides:
# - Cross-platform (Linux/Mac) compatibility
# - Common assertion functions
# - Test setup and cleanup utilities

set -euo pipefail

# Color definitions
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

# Test counters
TEST_PASSED=0
TEST_FAILED=0
TEST_SKIPPED=0

# ============================================
# Platform Detection
# ============================================

is_linux() {
  [[ "$OSTYPE" == "linux-gnu"* ]]
}

is_mac() {
  [[ "$OSTYPE" == "darwin"* ]]
}

# ============================================
# Mount/Unmount Utilities (Linux/Mac compatible)
# ============================================

is_mounted() {
  local mount_point="$1"
  if is_linux; then
    grep -q " $mount_point " /proc/mounts 2>/dev/null
  else
    mount | grep -q " on $mount_point " 2>/dev/null
  fi
}

detect_fusermount() {
  if command -v fusermount3 >/dev/null 2>&1; then
    echo "fusermount3"
  elif command -v fusermount >/dev/null 2>&1; then
    echo "fusermount"
  else
    echo ""
  fi
}

unmount_wsfs() {
  local mount_point="$1"
  local fusermount_cmd
  fusermount_cmd=$(detect_fusermount)

  if is_mounted "$mount_point"; then
    if [ -n "$fusermount_cmd" ]; then
      "$fusermount_cmd" -u "$mount_point" 2>/dev/null || true
    else
      umount "$mount_point" 2>/dev/null || true
    fi
  fi
}

wait_for_mount() {
  local mount_point="$1"
  local timeout="${2:-30}"

  for _ in $(seq 1 "$timeout"); do
    if is_mounted "$mount_point"; then
      return 0
    fi
    sleep 1
  done

  echo -e "${RED}Error: Mount did not become ready at $mount_point${NC}"
  return 1
}

# ============================================
# Cross-platform stat functions
# ============================================

stat_size() {
  if is_linux; then
    stat -c %s "$1" 2>/dev/null
  else
    stat -f %z "$1" 2>/dev/null
  fi
}

stat_mtime() {
  if is_linux; then
    stat -c %Y "$1" 2>/dev/null
  else
    stat -f %m "$1" 2>/dev/null
  fi
}

stat_atime() {
  if is_linux; then
    stat -c %X "$1" 2>/dev/null
  else
    stat -f %a "$1" 2>/dev/null
  fi
}

# ============================================
# Output Utilities
# ============================================

run_cmd() {
  echo -e "${YELLOW}\$ $1${NC}"
  eval "$1"
}

print_section() {
  local title="$1"
  echo ""
  echo "========================================"
  echo "$title"
  echo "========================================"
}

# ============================================
# Assertion Functions
# ============================================

assert() {
  local condition="$1"
  local description="$2"

  if eval "$condition"; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
    ((TEST_PASSED++)) || true
    return 0
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    ((TEST_FAILED++)) || true
    return 1
  fi
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local description="$3"

  if [ "$expected" = "$actual" ]; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
    ((TEST_PASSED++)) || true
    return 0
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    echo -e "  Expected: $expected"
    echo -e "  Actual:   $actual"
    ((TEST_FAILED++)) || true
    return 1
  fi
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local description="$3"

  if echo "$haystack" | grep -q "$needle"; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
    ((TEST_PASSED++)) || true
    return 0
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    echo -e "  Expected to contain: $needle"
    echo -e "  Actual: $haystack"
    ((TEST_FAILED++)) || true
    return 1
  fi
}

assert_file_exists() {
  local filepath="$1"
  local description="${2:-File $filepath exists}"
  assert "[ -f '$filepath' ]" "$description"
}

assert_dir_exists() {
  local dirpath="$1"
  local description="${2:-Directory $dirpath exists}"
  assert "[ -d '$dirpath' ]" "$description"
}

assert_not_exists() {
  local path="$1"
  local description="${2:-$path does not exist}"
  assert "[ ! -e '$path' ]" "$description"
}

assert_exit_code() {
  local expected_code="$1"
  local command="$2"
  local description="$3"

  set +e
  eval "$command" >/dev/null 2>&1
  local actual_code=$?
  set -e

  if [ "$expected_code" -eq "$actual_code" ]; then
    echo -e "${GREEN}✓ PASS:${NC} $description"
    ((TEST_PASSED++)) || true
    return 0
  else
    echo -e "${RED}✗ FAIL:${NC} $description"
    echo -e "  Expected exit code: $expected_code"
    echo -e "  Actual exit code:   $actual_code"
    ((TEST_FAILED++)) || true
    return 1
  fi
}

skip_test() {
  local reason="$1"
  echo -e "${YELLOW}⊘ SKIP:${NC} $reason"
  ((TEST_SKIPPED++)) || true
}

# ============================================
# Test Setup/Cleanup Utilities
# ============================================

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

  echo "Test directory: ${TEST_BASE_DIR}"
}

cleanup_test_dir() {
  local test_dir="${1:-$TEST_BASE_DIR}"

  if [ -n "$test_dir" ] && [ -d "$test_dir" ]; then
    echo ""
    echo "Cleaning up: $test_dir"
    cd /
    rm -rf "$test_dir" 2>/dev/null || true
  fi
}

# ============================================
# Cache Utilities
# ============================================

count_cache_entries() {
  local cache_dir="$1"
  if [ -d "$cache_dir" ]; then
    find "$cache_dir" -type f 2>/dev/null | wc -l | tr -d ' '
  else
    echo "0"
  fi
}

clean_cache_dir() {
  local cache_dir="$1"
  if [ -d "$cache_dir" ]; then
    rm -rf "${cache_dir:?}"/* 2>/dev/null || true
  else
    mkdir -p "$cache_dir"
  fi
}

# ============================================
# Test Summary
# ============================================

print_test_summary() {
  echo ""
  echo "========================================"
  echo "Test Summary"
  echo "========================================"
  echo -e "${GREEN}Passed:${NC}  $TEST_PASSED"
  echo -e "${RED}Failed:${NC}  $TEST_FAILED"
  echo -e "${YELLOW}Skipped:${NC} $TEST_SKIPPED"
  echo "========================================"

  if [ "$TEST_FAILED" -gt 0 ]; then
    echo -e "${RED}SOME TESTS FAILED${NC}"
    return 1
  else
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
    return 0
  fi
}
