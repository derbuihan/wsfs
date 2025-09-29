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

# Append to file
run_cmd 'echo "Appended line" >> hello.txt'
LINES=$(wc -l < hello.txt)
assert "[ $LINES -eq 3 ]" "File append works (3 lines expected due to echo adding newlines)"

# Verify line count another way
LINE_COUNT=$(grep -c . hello.txt || true)
assert "[ $LINE_COUNT -eq 2 ]" "File has 2 non-empty lines"

# Create empty file with touch
run_cmd 'touch empty.txt'
assert "[ -f empty.txt ]" "Empty file created with touch"
assert "[ ! -s empty.txt ]" "Empty file has zero size"

# Read with head/tail
run_cmd 'echo -e "line1\nline2\nline3\nline4\nline5" > multiline.txt'
FIRST_LINE=$(head -n 1 multiline.txt)
assert_eq "line1" "$FIRST_LINE" "head command works"
LAST_LINE=$(tail -n 1 multiline.txt)
assert_eq "line5" "$LAST_LINE" "tail command works"

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

# Create nested directories
run_cmd 'mkdir -p nested/deep/path'
assert "[ -d nested/deep/path ]" "Nested directories created with mkdir -p"

# List directory
run_cmd 'touch testdir/file1.txt testdir/file2.txt'
FILE_COUNT=$(ls testdir | wc -l)
assert "[ $FILE_COUNT -eq 2 ]" "ls command lists files correctly"

# ls -la shows details
run_cmd 'ls -la testdir > /dev/null'
assert "[ $? -eq 0 ]" "ls -la command works"

# Remove empty directory
run_cmd 'mkdir emptydir'
run_cmd 'rmdir emptydir'
assert "[ ! -d emptydir ]" "Empty directory removed with rmdir"

# Remove directory with contents
run_cmd 'mkdir -p fulldir/subdir'
run_cmd 'touch fulldir/file.txt fulldir/subdir/file.txt'
run_cmd 'rm -r fulldir'
assert "[ ! -d fulldir ]" "Directory with contents removed with rm -r"

# find command
run_cmd 'mkdir -p findtest/sub1/sub2'
run_cmd 'touch findtest/file1.txt findtest/sub1/file2.txt findtest/sub1/sub2/file3.txt'
FIND_COUNT=$(find findtest -name "*.txt" | wc -l)
assert "[ $FIND_COUNT -eq 3 ]" "find command locates all files"

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

# Move file to different directory
run_cmd 'mkdir movedir'
run_cmd 'mv renamed.txt movedir/'
assert "[ ! -f renamed.txt ]" "File moved from source"
assert "[ -f movedir/renamed.txt ]" "File exists in destination"

# Move and rename simultaneously
run_cmd 'mkdir movedir2'
run_cmd 'mv movedir/renamed.txt movedir2/final.txt'
assert "[ ! -f movedir/renamed.txt ]" "File moved from original location"
assert "[ -f movedir2/final.txt ]" "File exists with new name in new location"

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

# Check directory stat
run_cmd 'mkdir metadir'
run_cmd 'stat metadir > /dev/null'
assert "[ $? -eq 0 ]" "stat command works on directory"

# ls -l output format
run_cmd 'ls -l metafile.txt > /dev/null'
assert "[ $? -eq 0 ]" "ls -l displays file details"

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

# cd into non-existent directory
set +e
cd nonexistentdir 2>/dev/null
RESULT=$?
cd "${TEST_BASE_DIR}"
set -e
assert "[ $RESULT -ne 0 ]" "cd into non-existent directory returns error"

# cat a directory (should fail)
run_cmd 'mkdir catdir'
set +e
cat catdir 2>/dev/null
RESULT=$?
set -e
assert "[ $RESULT -ne 0 ]" "cat on directory returns error"

# cd into a file (should fail)
run_cmd 'touch regularfile'
set +e
cd regularfile 2>/dev/null
RESULT=$?
cd "${TEST_BASE_DIR}"
set -e
assert "[ $RESULT -ne 0 ]" "cd into file returns error"

# rmdir on a file (should fail)
set +e
rmdir regularfile 2>/dev/null
RESULT=$?
set -e
assert "[ $RESULT -ne 0 ]" "rmdir on file returns error"

# rmdir on non-empty directory (should fail)
run_cmd 'mkdir nonemptydir'
run_cmd 'touch nonemptydir/file.txt'
set +e
rmdir nonemptydir 2>/dev/null
RESULT=$?
set -e
assert "[ $RESULT -ne 0 ]" "rmdir on non-empty directory returns error"

# Files with spaces in name
run_cmd 'touch "file with spaces.txt"'
assert "[ -f 'file with spaces.txt' ]" "File with spaces in name created"
run_cmd 'echo "content" > "file with spaces.txt"'
CONTENT=$(cat "file with spaces.txt")
assert_eq "content" "$CONTENT" "File with spaces can be read"

# Files with special characters
run_cmd 'touch "file-with_special.chars.txt"'
assert "[ -f 'file-with_special.chars.txt' ]" "File with special characters created"

echo ""

# ========================================
# TEST 6: Real-world Use Cases
# ========================================
echo "========================================"
echo "TEST 6: Real-world Use Cases"
echo "========================================"

# Copy operations
run_cmd 'echo "source content" > source.txt'
run_cmd 'cp source.txt destination.txt'
assert "[ -f destination.txt ]" "File copied successfully"
CONTENT=$(cat destination.txt)
assert_eq "source content" "$CONTENT" "Copied file has correct content"

# grep operation
run_cmd 'echo -e "apple\nbanana\ncherry\napricot" > fruits.txt'
GREP_COUNT=$(grep -c "^a" fruits.txt)
assert "[ $GREP_COUNT -eq 2 ]" "grep search works correctly"

# awk operation
run_cmd 'echo -e "1 2 3\n4 5 6\n7 8 9" > numbers.txt'
SUM=$(awk '{sum+=$2} END {print sum}' numbers.txt)
assert "[ $SUM -eq 15 ]" "awk processing works (2+5+8=15)"

# sed operation
run_cmd 'echo "Hello World" > sed_test.txt'
run_cmd 'sed -i.bak "s/World/FUSE/" sed_test.txt'
CONTENT=$(cat sed_test.txt)
assert_eq "Hello FUSE" "$CONTENT" "sed file editing works"

# Multiple files processing
run_cmd 'mkdir bulktest'
run_cmd 'for i in {1..10}; do echo "file $i" > bulktest/file$i.txt; done'
COUNT=$(ls bulktest | wc -l)
assert "[ $COUNT -eq 10 ]" "Multiple files created successfully"

# wc command
run_cmd 'echo -e "line1\nline2\nline3" > wc_test.txt'
LINE_COUNT=$(wc -l < wc_test.txt)
assert "[ $LINE_COUNT -eq 3 ]" "wc line count works"

echo ""

# ========================================
# TEST 7: Concurrent Operations
# ========================================
echo "========================================"
echo "TEST 7: Concurrent Operations"
echo "========================================"

# Multiple simultaneous reads
run_cmd 'echo "concurrent read test" > concurrent.txt'
run_cmd 'cat concurrent.txt > /dev/null & cat concurrent.txt > /dev/null & cat concurrent.txt > /dev/null & wait'
assert "[ $? -eq 0 ]" "Multiple concurrent reads succeed"

# Create multiple files concurrently
run_cmd 'mkdir concurrent_dir'
run_cmd 'cd concurrent_dir && (touch file1.txt & touch file2.txt & touch file3.txt & wait) && cd ..'
COUNT=$(ls concurrent_dir | wc -l)
assert "[ $COUNT -eq 3 ]" "Concurrent file creation succeeds"

echo ""

# ========================================
# TEST 8: Performance and Large Files
# ========================================
echo "========================================"
echo "TEST 8: Performance and Large Files"
echo "========================================"

# Create a moderately large file (1MB)
run_cmd 'dd if=/dev/zero of=largefile.txt bs=1024 count=1024 2>/dev/null'
assert "[ -f largefile.txt ]" "Large file (1MB) created"
SIZE=$(stat -c %s largefile.txt 2>/dev/null || stat -f %z largefile.txt 2>/dev/null)
assert "[ $SIZE -eq 1048576 ]" "Large file has correct size (1048576 bytes)"

# Read large file
run_cmd 'cat largefile.txt > /dev/null'
assert "[ $? -eq 0 ]" "Large file read successfully"

# Partial read with dd
run_cmd 'dd if=largefile.txt of=partial.txt bs=1024 count=10 2>/dev/null'
SIZE=$(stat -c %s partial.txt 2>/dev/null || stat -f %z partial.txt 2>/dev/null)
assert "[ $SIZE -eq 10240 ]" "Partial read works correctly (10240 bytes)"

# Directory with many files
run_cmd 'mkdir manyfiles'
run_cmd 'cd manyfiles && for i in {1..100}; do touch file$i.txt; done && cd ..'
COUNT=$(ls manyfiles | wc -l)
assert "[ $COUNT -eq 100 ]" "Directory with 100 files handled correctly"

echo ""

# ========================================
# TEST 9: Binary Files
# ========================================
echo "========================================"
echo "TEST 9: Binary Files"
echo "========================================"

# Create and verify binary file
run_cmd 'echo -e "\x00\x01\x02\x03\x04" > binary.dat'
assert "[ -f binary.dat ]" "Binary file created"
SIZE=$(stat -c %s binary.dat 2>/dev/null || stat -f %z binary.dat 2>/dev/null)
assert "[ $SIZE -gt 0 ]" "Binary file has content"

echo ""

# ========================================
# All Tests Passed
# ========================================
echo "========================================"
echo -e "${GREEN}ALL TESTS PASSED!${NC}"
echo "========================================"
echo "Total test categories: 9"
echo "Test directory will be cleaned up automatically"
echo ""
