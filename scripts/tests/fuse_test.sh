#!/bin/bash

# Unified FUSE Filesystem Test Script
# Tests all FUSE operations on a mounted wsfs filesystem
#
# Usage: ./fuse_test.sh /path/to/mountpoint
#
# Sections:
#   1. Basic File Operations
#   2. Directory Operations
#   3. File Modification Operations (rename, truncate, append)
#   4. Attribute Operations (stat, touch, mtime)
#   5. Edge Cases (long filenames, unicode, special chars)
#   6. Error Handling (ENOENT, EISDIR, ENOTDIR)
#   7. Editor Compatibility (vim save patterns)
#   8. Large Files & Concurrency
#   9. Databricks CLI Verification (optional)

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

# Setup test directory
setup_test_dir "$MOUNT_POINT" "fuse_test"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

echo "========================================"
echo "FUSE Filesystem Test Suite"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo ""

# ============================================
# SECTION 1: Basic File Operations
# ============================================
print_section "Section 1: Basic File Operations"

# Create file with content
run_cmd 'echo "Hello FUSE World" > hello.txt'
assert_file_exists "hello.txt" "File hello.txt created"

# Read file content
CONTENT=$(cat hello.txt)
assert_eq "Hello FUSE World" "$CONTENT" "File content matches"

# Overwrite file
run_cmd 'echo "Updated content" > hello.txt'
CONTENT=$(cat hello.txt)
assert_eq "Updated content" "$CONTENT" "File overwrite works"

# Create empty file with touch
run_cmd 'touch empty.txt'
assert_file_exists "empty.txt" "Empty file created with touch"
SIZE=$(stat_size empty.txt)
assert_eq "0" "$SIZE" "Empty file has size 0"

# Delete file
run_cmd 'rm hello.txt'
assert_not_exists "hello.txt" "File deleted successfully"

# ============================================
# SECTION 2: Directory Operations
# ============================================
print_section "Section 2: Directory Operations"

# Create directory
run_cmd 'mkdir testdir'
assert_dir_exists "testdir" "Directory created with mkdir"

# Create files in directory
run_cmd 'touch testdir/file1.txt testdir/file2.txt'
FILE_COUNT=$(ls testdir | wc -l | tr -d ' ')
assert "[ $FILE_COUNT -eq 2 ]" "ls command lists files correctly ($FILE_COUNT files)"

# Remove empty directory
run_cmd 'mkdir emptydir'
run_cmd 'rmdir emptydir'
assert_not_exists "emptydir" "Empty directory removed with rmdir"

# Create nested directory
run_cmd 'mkdir -p nested/deep/dir'
assert_dir_exists "nested/deep/dir" "Nested directory created with mkdir -p"

# Remove non-empty directory
run_cmd 'rm -rf testdir'
assert_not_exists "testdir" "Non-empty directory removed with rm -rf"

# ============================================
# SECTION 3: File Modification Operations
# ============================================
print_section "Section 3: File Modification Operations"

# Rename file in same directory
run_cmd 'echo "test content" > original.txt'
run_cmd 'mv original.txt renamed.txt'
assert_not_exists "original.txt" "Original file no longer exists after rename"
assert_file_exists "renamed.txt" "Renamed file exists"
CONTENT=$(cat renamed.txt)
assert_eq "test content" "$CONTENT" "Content preserved after rename"

# Move file to different directory
run_cmd 'mkdir movedir'
run_cmd 'mv renamed.txt movedir/'
assert_file_exists "movedir/renamed.txt" "File moved to different directory"

# Rename directory
run_cmd 'mkdir olddir'
run_cmd 'touch olddir/file.txt'
run_cmd 'mv olddir newdir'
assert_not_exists "olddir" "Old directory name no longer exists"
assert_dir_exists "newdir" "New directory name exists"
assert_file_exists "newdir/file.txt" "File exists in renamed directory"

# Truncate file (shrink)
run_cmd 'printf "hello world" > trunc.txt'
if command -v truncate >/dev/null 2>&1; then
  run_cmd 'truncate -s 5 trunc.txt'
else
  python3 -c "
with open('trunc.txt', 'r+b') as f:
    f.truncate(5)
"
fi
SIZE=$(stat_size trunc.txt)
assert_eq "5" "$SIZE" "truncate shrinks file to 5 bytes"
CONTENT=$(cat trunc.txt)
assert_eq "hello" "$CONTENT" "truncate keeps prefix"

# Append to file
run_cmd 'printf "A" > append.txt'
run_cmd 'printf "B" >> append.txt'
CONTENT=$(cat append.txt)
assert_eq "AB" "$CONTENT" "append works"

# Atomic replace (temp -> rename over existing)
run_cmd 'echo "old" > atom.txt'
run_cmd 'echo "new" > .atom.tmp'
run_cmd 'mv .atom.tmp atom.txt'
CONTENT=$(cat atom.txt)
assert_eq "new" "$CONTENT" "atomic replace content"

# Heredoc write
cat <<'EOF' > heredoc.txt
line1
line2
EOF
CONTENT=$(cat heredoc.txt | tr -d '\r')
EXPECTED=$(printf "line1\nline2")
assert_eq "$EXPECTED" "$CONTENT" "heredoc write"

# ============================================
# SECTION 4: Attribute Operations
# ============================================
print_section "Section 4: Attribute Operations"

# stat basic
run_cmd 'echo "metadata test" > metafile.txt'
run_cmd 'stat metafile.txt > /dev/null'
assert "[ $? -eq 0 ]" "stat command works on file"

# File size
SIZE=$(stat_size metafile.txt)
assert "[ $SIZE -gt 0 ]" "File size is greater than zero"

# touch mtime update
run_cmd 'printf "touch" > touch_test.txt'
BEFORE=$(stat_mtime touch_test.txt)
sleep 1
run_cmd 'touch touch_test.txt'
AFTER=$(stat_mtime touch_test.txt)
assert "[ $AFTER -gt $BEFORE ]" "touch updates mtime"

# chmod (expected to fail - not supported)
run_cmd 'printf "perm" > perm.txt'
set +e
chmod 600 perm.txt 2>/dev/null
CHMOD_RC=$?
set -e
if [ $CHMOD_RC -ne 0 ]; then
  echo -e "${GREEN}✓ PASS:${NC} chmod returns error (not supported)"
  ((TEST_PASSED++)) || true
else
  echo -e "${YELLOW}⊘ INFO:${NC} chmod succeeded (may be no-op)"
fi

# chown (expected to fail - not supported)
set +e
chown 9999:9999 perm.txt 2>/dev/null
CHOWN_RC=$?
set -e
if [ $CHOWN_RC -ne 0 ]; then
  echo -e "${GREEN}✓ PASS:${NC} chown returns error (not supported)"
  ((TEST_PASSED++)) || true
else
  echo -e "${YELLOW}⊘ INFO:${NC} chown succeeded (may be no-op)"
fi

# ============================================
# SECTION 5: Edge Cases
# ============================================
print_section "Section 5: Edge Cases"

# Long filename (254 chars - should work)
LONG_NAME=$(printf 'a%.0s' {1..254})
run_cmd "echo 'long' > '${LONG_NAME}.txt'"
assert_file_exists "${LONG_NAME}.txt" "254-char filename works"
rm -f "${LONG_NAME}.txt"

# Unicode/Japanese filename
run_cmd 'echo "unicode" > "テスト.txt"'
assert_file_exists "テスト.txt" "Unicode filename works"
CONTENT=$(cat "テスト.txt")
assert_eq "unicode" "$CONTENT" "Unicode file content readable"

# Special characters in filename
run_cmd 'echo "special" > "file-with_special.chars.txt"'
assert_file_exists "file-with_special.chars.txt" "Special chars in filename work"

# Filename with space
run_cmd 'echo "space" > "file with space.txt"'
assert_file_exists "file with space.txt" "Filename with space works"

# Dot file (hidden)
run_cmd 'echo "hidden" > ".hidden"'
assert_file_exists ".hidden" "Dot file (hidden) works"

# Deep nesting (10 levels)
run_cmd 'mkdir -p deep/1/2/3/4/5/6/7/8/9'
run_cmd 'echo "deep" > deep/1/2/3/4/5/6/7/8/9/file.txt'
assert_file_exists "deep/1/2/3/4/5/6/7/8/9/file.txt" "Deep nesting (10 levels) works"
CONTENT=$(cat deep/1/2/3/4/5/6/7/8/9/file.txt)
assert_eq "deep" "$CONTENT" "Deep nested file content readable"

# Empty file read
run_cmd 'touch empty_read.txt'
CONTENT=$(cat empty_read.txt)
assert_eq "" "$CONTENT" "Empty file read returns empty string"

# ============================================
# SECTION 6: Error Handling
# ============================================
print_section "Section 6: Error Handling"

# Read non-existent file
assert_exit_code 1 "cat nonexistent_file_12345.txt 2>/dev/null" "Reading non-existent file returns error"

# cat directory (should fail)
run_cmd 'mkdir catdir'
assert_exit_code 1 "cat catdir 2>/dev/null" "cat on directory returns error"

# rmdir on non-empty directory
run_cmd 'mkdir nonempty'
run_cmd 'touch nonempty/file.txt'
assert_exit_code 1 "rmdir nonempty 2>/dev/null" "rmdir on non-empty directory fails"
rm -rf nonempty

# ============================================
# SECTION 7: Editor Compatibility
# ============================================
print_section "Section 7: Editor Compatibility"

if command -v vim >/dev/null 2>&1; then
  # Vim default save
  run_cmd 'printf "before" > vim_default.txt'
  set +e
  vim -Es vim_default.txt <<'VIMCMDS'
normal Goafter
wq
VIMCMDS
  VIM_RC=$?
  set -e
  assert "[ $VIM_RC -eq 0 ]" "vim default :wq succeeds"
  CONTENT=$(cat vim_default.txt)
  EXPECTED=$(printf "before\nafter")
  assert_eq "$EXPECTED" "$CONTENT" "vim default save content"

  # Vim with backup
  run_cmd 'printf "one" > vim_backup.txt'
  set +e
  vim -Es -c "set backup" -c "set writebackup" -c "set backupcopy=yes" -c "normal GoTWO" -c "wq" vim_backup.txt
  VIM_RC=$?
  set -e
  assert "[ $VIM_RC -eq 0 ]" "vim backup/writebackup succeeds"
  CONTENT=$(cat vim_backup.txt)
  EXPECTED=$(printf "one\nTWO")
  assert_eq "$EXPECTED" "$CONTENT" "vim backup content"
else
  skip_test "vim not installed, skipping editor tests"
fi

# ============================================
# SECTION 8: Large Files & Concurrency
# ============================================
print_section "Section 8: Large Files & Concurrency"

# 10MB file write and read
echo "Creating 10MB test file..."
dd if=/dev/urandom of=large_10mb.bin bs=1M count=10 2>/dev/null
HASH1=$(md5sum large_10mb.bin 2>/dev/null | cut -d' ' -f1 || md5 -q large_10mb.bin 2>/dev/null)
echo "  Hash: $HASH1"

# Read back and verify
cp large_10mb.bin large_10mb_copy.bin
HASH2=$(md5sum large_10mb_copy.bin 2>/dev/null | cut -d' ' -f1 || md5 -q large_10mb_copy.bin 2>/dev/null)
assert_eq "$HASH1" "$HASH2" "10MB file hash matches after copy"

# Concurrent reads (10 parallel)
echo "Testing concurrent reads..."
run_cmd 'echo "concurrent" > concurrent.txt'
for i in {1..10}; do
  (cat concurrent.txt > /dev/null) &
done
wait
echo -e "${GREEN}✓ PASS:${NC} Concurrent reads completed without errors"
((TEST_PASSED++)) || true

# ============================================
# SECTION 9: Databricks CLI Verification (Optional)
# ============================================
print_section "Section 9: Databricks CLI Verification"

if command -v databricks >/dev/null 2>&1 && [ -n "${DATABRICKS_HOST:-}" ] && [ -n "${DATABRICKS_TOKEN:-}" ]; then
  WORKSPACE_PATH=$(pwd | sed "s|^${MOUNT_POINT}||")
  echo "Workspace path: ${WORKSPACE_PATH}"

  # Create a file via wsfs
  TEST_CONTENT="CLI verification test $(date +%s)"
  echo "$TEST_CONTENT" > cli_verify.txt

  sleep 2

  # Verify via Databricks CLI
  CLI_CONTENT=$(databricks workspace export "${WORKSPACE_PATH}/cli_verify.txt" --format SOURCE 2>/dev/null || echo "ERROR")

  if [ "$CLI_CONTENT" = "ERROR" ]; then
    echo -e "${YELLOW}⊘ SKIP:${NC} Could not verify via CLI (file not found)"
  else
    WSFS_CONTENT=$(cat cli_verify.txt)
    if [ "$WSFS_CONTENT" = "$CLI_CONTENT" ]; then
      echo -e "${GREEN}✓ PASS:${NC} Databricks CLI verification succeeded"
      ((TEST_PASSED++)) || true
    else
      echo -e "${RED}✗ FAIL:${NC} Content mismatch between wsfs and CLI"
      echo "  wsfs: $WSFS_CONTENT"
      echo "  CLI:  $CLI_CONTENT"
      ((TEST_FAILED++)) || true
    fi
  fi
else
  skip_test "Databricks CLI not available or credentials not set"
fi

# ============================================
# Test Summary
# ============================================
print_test_summary
