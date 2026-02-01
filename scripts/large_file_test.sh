#!/bin/bash
set -e

MOUNT_POINT="${1:-/mnt/wsfs}"
TEST_DIR="$MOUNT_POINT/wsfs_large_file_test_$$"

echo "=== Large File Test for wsfs ==="
echo "Mount point: $MOUNT_POINT"
echo "Test directory: $TEST_DIR"
echo

# Create test directory
echo "Creating test directory: $TEST_DIR"
mkdir -p "$TEST_DIR"

# Test 10MB file
echo "Testing 10MB file..."
dd if=/dev/urandom of=/tmp/test_10mb.bin bs=1M count=10 2>/dev/null
echo "  - Writing 10MB file to wsfs..."
cp /tmp/test_10mb.bin "$TEST_DIR/test_10mb.bin"
echo "  - Reading back and comparing..."
diff /tmp/test_10mb.bin "$TEST_DIR/test_10mb.bin"
echo "  ✓ 10MB file test passed"
echo

# Test 50MB file (optional, controlled by environment variable)
if [ "${LARGE_FILE_TEST:-}" = "1" ]; then
    echo "Testing 50MB file..."
    dd if=/dev/urandom of=/tmp/test_50mb.bin bs=1M count=50 2>/dev/null
    echo "  - Writing 50MB file to wsfs..."
    cp /tmp/test_50mb.bin "$TEST_DIR/test_50mb.bin"
    echo "  - Reading back and comparing..."
    diff /tmp/test_50mb.bin "$TEST_DIR/test_50mb.bin"
    echo "  ✓ 50MB file test passed"
    echo
fi

# Test 100MB file (optional, controlled by environment variable)
if [ "${LARGE_FILE_TEST:-}" = "2" ]; then
    echo "Testing 100MB file..."
    dd if=/dev/urandom of=/tmp/test_100mb.bin bs=1M count=100 2>/dev/null
    echo "  - Writing 100MB file to wsfs..."
    cp /tmp/test_100mb.bin "$TEST_DIR/test_100mb.bin"
    echo "  - Reading back and comparing..."
    diff /tmp/test_100mb.bin "$TEST_DIR/test_100mb.bin"
    echo "  ✓ 100MB file test passed"
    echo
fi

# Test streaming read (read in chunks)
echo "Testing streaming read (10MB file in 1MB chunks)..."
dd if="$TEST_DIR/test_10mb.bin" of=/tmp/test_10mb_read.bin bs=1M 2>/dev/null
diff /tmp/test_10mb.bin /tmp/test_10mb_read.bin
echo "  ✓ Streaming read test passed"
echo

# Cleanup
echo "Cleaning up..."
rm -rf "$TEST_DIR"
rm -f /tmp/test_10mb.bin /tmp/test_10mb_read.bin
if [ "${LARGE_FILE_TEST:-}" = "1" ]; then
    rm -f /tmp/test_50mb.bin
fi
if [ "${LARGE_FILE_TEST:-}" = "2" ]; then
    rm -f /tmp/test_100mb.bin
fi

echo "=== All large file tests passed successfully ==="
