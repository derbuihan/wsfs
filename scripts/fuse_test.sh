#!/bin/bash

# FUSE Filesystem Test Script
# This script comprehensively tests FUSE filesystem operations

# Setting

set -euo pipefail

GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
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

stat_size() {
  stat -c %s "$1" 2>/dev/null || stat -f %z "$1" 2>/dev/null
}

stat_mtime() {
  stat -c %Y "$1" 2>/dev/null || stat -f %m "$1" 2>/dev/null
}

# Setup

MOUNT_POINT=$1

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

TEST_BASE_DIR_NAME="fuse_test_$(date +%s)_$$"
TEST_BASE_DIR="${MOUNT_POINT}/${TEST_BASE_DIR_NAME}"

echo "========================================"
echo "FUSE Filesystem Test Suite"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo ""

echo "Setting up test directory..."
mkdir -p "${TEST_BASE_DIR}"
cd "${TEST_BASE_DIR}"
echo "Running tests in $(pwd)"
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
# TEST 1: Basic File Operations - Read/Write
# ========================================
echo "========================================"
echo "TEST 1: Basic File Operations"
echo "========================================"

# Create file with content
run_cmd 'echo "Hello FUSE World" > hello.txt'
assert "[ -f hello.txt ]" "File hello.txt exists"

# Read file content
CONTENT=$(cat hello.txt)
assert_eq "Hello FUSE World" "$CONTENT" "File content matches"

# Overwrite file
run_cmd 'echo "Updated content" > hello.txt'
CONTENT=$(cat hello.txt)
assert_eq "Updated content" "$CONTENT" "File overwrite works"

# Delete file
run_cmd 'rm hello.txt'
assert "[ ! -f hello.txt ]" "File deleted successfully"

echo ""

# ========================================
# TEST 2: Directory Operations
# ========================================
echo "========================================"
echo "TEST 2: Directory Operations"
echo "========================================"

# Create single directory
run_cmd 'mkdir testdir'
assert "[ -d testdir ]" "Directory created with mkdir"

# List directory
run_cmd 'touch testdir/file1.txt testdir/file2.txt'
FILE_COUNT=$(ls testdir | wc -l)
assert "[ $FILE_COUNT -eq 2 ]" "ls command lists files correctly"

# Remove empty directory
run_cmd 'mkdir emptydir'
run_cmd 'rmdir emptydir'
assert "[ ! -d emptydir ]" "Empty directory removed with rmdir"

echo ""

# ========================================
# TEST 3: File Rename and Move
# ========================================
echo "========================================"
echo "TEST 3: File Rename and Move"
echo "========================================"

# Rename file in same directory
run_cmd 'echo "test content" > original.txt'
run_cmd 'mv original.txt renamed.txt'
assert "[ ! -f original.txt ]" "Original file no longer exists after rename"
assert "[ -f renamed.txt ]" "Renamed file exists"
CONTENT=$(cat renamed.txt)
assert_eq "test content" "$CONTENT" "Content preserved after rename"

# Rename directory
run_cmd 'mkdir olddir'
run_cmd 'touch olddir/file.txt'
run_cmd 'mv olddir newdir'
assert "[ ! -d olddir ]" "Old directory name no longer exists"
assert "[ -d newdir ]" "New directory name exists"
assert "[ -f newdir/file.txt ]" "File exists in renamed directory"

echo ""

# ========================================
# TEST 4: File Attributes and Metadata
# ========================================
echo "========================================"
echo "TEST 4: File Attributes and Metadata"
echo "========================================"

# Create file and check stat
run_cmd 'echo "metadata test" > metafile.txt'
run_cmd 'stat metafile.txt > /dev/null'
assert "[ $? -eq 0 ]" "stat command works on file"

# Check file size
SIZE=$(stat -c %s metafile.txt 2>/dev/null || stat -f %z metafile.txt 2>/dev/null)
assert "[ $SIZE -gt 0 ]" "File size is greater than zero"

echo ""

# ========================================
# TEST 5: Edge Cases and Error Handling
# ========================================
echo "========================================"
echo "TEST 5: Edge Cases and Error Handling"
echo "========================================"

# Access non-existent file
set +e
cat nonexistent.txt 2>/dev/null
RESULT=$?
set -e
assert "[ $RESULT -ne 0 ]" "Reading non-existent file returns error"

# cat a directory (should fail)
run_cmd 'mkdir catdir'
set +e
cat catdir 2>/dev/null
RESULT=$?
set -e
assert "[ $RESULT -ne 0 ]" "cat on directory returns error"

# ========================================
# TEST 6: Save Methods (truncate/atomic/append)
# ========================================
echo "========================================"
echo "TEST 6: Save Methods"
echo "========================================"

# Truncate file
run_cmd 'printf "hello world" > trunc.txt'
if command -v truncate >/dev/null 2>&1; then
  run_cmd 'truncate -s 5 trunc.txt'
else
  run_cmd 'python3 - <<'\''PY'\''
with open("trunc.txt", "r+b") as f:
    f.truncate(5)
PY'
fi
SIZE=$(stat_size trunc.txt)
assert_eq "5" "$SIZE" "truncate shrinks file"
CONTENT=$(cat trunc.txt)
assert_eq "hello" "$CONTENT" "truncate keeps prefix"

# Atomic replace (temp -> rename over existing)
run_cmd 'echo "old" > atom.txt'
run_cmd 'echo "new" > .atom.tmp'
run_cmd 'mv .atom.tmp atom.txt'
CONTENT=$(cat atom.txt)
assert_eq "new" "$CONTENT" "atomic replace content"

# Append
run_cmd 'printf "A" > append.txt'
run_cmd 'printf "B" >> append.txt'
CONTENT=$(cat append.txt)
assert_eq "AB" "$CONTENT" "append works"

# Heredoc write
cat <<'EOF' > heredoc.txt
line1
line2
EOF
CONTENT=$(cat heredoc.txt | tr -d '\r')
EXPECTED=$(printf "line1\nline2")
assert_eq "$EXPECTED" "$CONTENT" "heredoc write"

echo ""

# ========================================
# TEST 7: Vim Save and Touch
# ========================================
echo "========================================"
echo "TEST 7: Vim Save and Touch"
echo "========================================"

assert "command -v vim >/dev/null 2>&1" "vim is installed"

# Vim default save
run_cmd 'printf "before" > vim_default.txt'
set +e
vim -Es vim_default.txt <<'VIMCMDS'
normal Goafter
wq
VIMCMDS
VIM_RC=$?
set -e
assert "[ $VIM_RC -eq 0 ]" "vim default :wq"
CONTENT=$(cat vim_default.txt)
EXPECTED=$(printf "before\nafter")
assert_eq "$EXPECTED" "$CONTENT" "vim default content"

# Vim with backup/writebackup
run_cmd 'printf "one" > vim_backup.txt'
set +e
vim -Es -c "set backup" -c "set writebackup" -c "set backupcopy=yes" -c "normal GoTWO" -c "wq" vim_backup.txt
VIM_RC=$?
set -e
assert "[ $VIM_RC -eq 0 ]" "vim backup/writebackup"
CONTENT=$(cat vim_backup.txt)
EXPECTED=$(printf "one\nTWO")
assert_eq "$EXPECTED" "$CONTENT" "vim backup content"
if [ -f vim_backup.txt~ ]; then
  echo -e "${YELLOW}INFO:${NC} vim backup file created (vim_backup.txt~)"
else
  echo -e "${YELLOW}INFO:${NC} vim backup file not created"
fi

# Vim swapfile enabled (should be cleaned on exit)
run_cmd 'printf "a" > vim_swap.txt'
set +e
vim -Es -c "set swapfile" -c "normal GoB" -c "wq" vim_swap.txt
VIM_RC=$?
set -e
assert "[ $VIM_RC -eq 0 ]" "vim swapfile"
CONTENT=$(cat vim_swap.txt)
EXPECTED=$(printf "a\nB")
assert_eq "$EXPECTED" "$CONTENT" "vim swapfile content"
assert "[ ! -f .vim_swap.txt.swp ]" "swap file cleaned"

# touch mtime update
run_cmd 'printf "touch" > touch.txt'
BEFORE=$(stat_mtime touch.txt)
sleep 1
run_cmd 'touch touch.txt'
AFTER=$(stat_mtime touch.txt)
assert "[ $AFTER -gt $BEFORE ]" "touch updates mtime"

# ========================================
# All Tests Passed
# ========================================
echo "========================================"
echo -e "${GREEN}ALL TESTS PASSED!${NC}"
echo "========================================"
echo "Total test categories: 7"
echo "Test directory will be cleaned up automatically"
echo ""
