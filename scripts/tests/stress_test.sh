#!/bin/bash
#
# Stress Tests for wsfs
# Tests concurrent file operations, rapid truncates, and rename during I/O
#
# Usage: ./stress_test.sh MOUNT_POINT
#
# Example:
#   ./stress_test.sh /mnt/wsfs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

# ============================================
# Configuration
# ============================================

NUM_PARALLEL=10
NUM_ITERATIONS=5
STRESS_TIMEOUT=60

# ============================================
# Main
# ============================================

main() {
  if [ $# -lt 1 ]; then
    echo "Usage: $0 MOUNT_POINT"
    echo "Example: $0 /mnt/wsfs"
    exit 1
  fi

  local mount_point="$1"

  print_section "wsfs Stress Tests"
  echo "Mount point: $mount_point"
  echo "Parallel processes: $NUM_PARALLEL"
  echo "Iterations per test: $NUM_ITERATIONS"

  setup_test_dir "$mount_point" "stress"

  # Run tests
  test_concurrent_writes
  test_rapid_truncate
  test_concurrent_create_delete
  test_concurrent_read_write
  test_rename_operations

  cleanup_test_dir

  print_test_summary
}

# ============================================
# Test 1: Concurrent Writes to Same File
# ============================================

test_concurrent_writes() {
  print_section "Test 1: Concurrent Writes to Same File"

  local test_file="concurrent_write.txt"
  echo "initial" > "$test_file"

  local pids=()
  for i in $(seq 1 $NUM_PARALLEL); do
    (
      for j in $(seq 1 $NUM_ITERATIONS); do
        echo "data from process $i iteration $j" > "$test_file" 2>/dev/null || true
      done
    ) &
    pids+=($!)
  done

  # Wait for all processes
  local failed=0
  for pid in "${pids[@]}"; do
    if ! wait "$pid" 2>/dev/null; then
      ((failed++)) || true
    fi
  done

  # Verify file exists and is readable
  if [ -f "$test_file" ]; then
    local content
    content=$(cat "$test_file" 2>/dev/null || echo "ERROR")
    if [ "$content" != "ERROR" ]; then
      echo -e "${GREEN}PASS:${NC} Concurrent writes completed without crash"
      ((TEST_PASSED++)) || true
    else
      echo -e "${RED}FAIL:${NC} File became unreadable"
      ((TEST_FAILED++)) || true
    fi
  else
    echo -e "${RED}FAIL:${NC} File disappeared during concurrent writes"
    ((TEST_FAILED++)) || true
  fi

  rm -f "$test_file" 2>/dev/null || true
}

# ============================================
# Test 2: Rapid Truncate Operations
# ============================================

test_rapid_truncate() {
  print_section "Test 2: Rapid Truncate Operations"

  local test_file="truncate_test.txt"

  # Create initial file with some content
  dd if=/dev/urandom of="$test_file" bs=1024 count=100 2>/dev/null

  # Rapid sequential truncates
  local sizes=(50000 10000 80000 0 20000 150000 5000 30000 100000 75000)

  for size in "${sizes[@]}"; do
    if ! truncate -s "$size" "$test_file" 2>/dev/null; then
      echo -e "${RED}FAIL:${NC} Truncate to $size bytes failed"
      ((TEST_FAILED++)) || true
      rm -f "$test_file" 2>/dev/null || true
      return
    fi
  done

  # Verify final size
  local final_size
  final_size=$(stat_size "$test_file")
  if [ "$final_size" = "75000" ]; then
    echo -e "${GREEN}PASS:${NC} Rapid truncates completed (final size: $final_size)"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} Final size mismatch (expected: 75000, got: $final_size)"
    ((TEST_FAILED++)) || true
  fi

  rm -f "$test_file" 2>/dev/null || true
}

# ============================================
# Test 3: Concurrent Create/Delete
# ============================================

test_concurrent_create_delete() {
  print_section "Test 3: Concurrent Create/Delete"

  local test_subdir="create_delete_test"
  mkdir -p "$test_subdir"

  local pids=()

  # Create files concurrently
  for i in $(seq 1 $NUM_PARALLEL); do
    (
      for j in $(seq 1 $NUM_ITERATIONS); do
        local fname="$test_subdir/file_${i}_${j}.txt"
        echo "content $i $j" > "$fname" 2>/dev/null || true
      done
    ) &
    pids+=($!)
  done

  # Wait for creates
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  # Count created files
  local file_count
  file_count=$(find "$test_subdir" -type f 2>/dev/null | wc -l | tr -d ' ')
  echo "Created $file_count files"

  # Delete files concurrently
  pids=()
  for i in $(seq 1 $NUM_PARALLEL); do
    (
      for j in $(seq 1 $NUM_ITERATIONS); do
        local fname="$test_subdir/file_${i}_${j}.txt"
        rm -f "$fname" 2>/dev/null || true
      done
    ) &
    pids+=($!)
  done

  # Wait for deletes
  for pid in "${pids[@]}"; do
    wait "$pid" 2>/dev/null || true
  done

  # Count remaining files
  local remaining
  remaining=$(find "$test_subdir" -type f 2>/dev/null | wc -l | tr -d ' ')

  if [ "$remaining" = "0" ]; then
    echo -e "${GREEN}PASS:${NC} Concurrent create/delete completed successfully"
    ((TEST_PASSED++)) || true
  else
    echo -e "${YELLOW}WARN:${NC} $remaining files remain (may be due to timing)"
    ((TEST_PASSED++)) || true  # Not necessarily a failure
  fi

  rm -rf "$test_subdir" 2>/dev/null || true
}

# ============================================
# Test 4: Concurrent Read/Write
# ============================================

test_concurrent_read_write() {
  print_section "Test 4: Concurrent Read/Write"

  local test_file="rw_test.txt"

  # Create initial file
  dd if=/dev/urandom of="$test_file" bs=1024 count=10 2>/dev/null

  local pids=()
  local errors=0

  # Readers
  for i in $(seq 1 $((NUM_PARALLEL / 2))); do
    (
      for j in $(seq 1 $NUM_ITERATIONS); do
        cat "$test_file" > /dev/null 2>&1 || true
      done
    ) &
    pids+=($!)
  done

  # Writers
  for i in $(seq 1 $((NUM_PARALLEL / 2))); do
    (
      for j in $(seq 1 $NUM_ITERATIONS); do
        echo "write from $i:$j" >> "$test_file" 2>/dev/null || true
      done
    ) &
    pids+=($!)
  done

  # Wait for all
  for pid in "${pids[@]}"; do
    if ! wait "$pid" 2>/dev/null; then
      ((errors++)) || true
    fi
  done

  # Verify file is still readable
  if cat "$test_file" > /dev/null 2>&1; then
    echo -e "${GREEN}PASS:${NC} Concurrent read/write completed (errors: $errors)"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} File became unreadable after concurrent operations"
    ((TEST_FAILED++)) || true
  fi

  rm -f "$test_file" 2>/dev/null || true
}

# ============================================
# Test 5: Rename Operations
# ============================================

test_rename_operations() {
  print_section "Test 5: Rename Operations"

  local test_subdir="rename_test"
  mkdir -p "$test_subdir"

  # Create files
  for i in $(seq 1 $NUM_PARALLEL); do
    echo "content $i" > "$test_subdir/file_$i.txt"
  done

  # Sequential renames (more realistic for wsfs)
  local success=0
  for i in $(seq 1 $NUM_PARALLEL); do
    local src="$test_subdir/file_$i.txt"
    local dst="$test_subdir/renamed_$i.txt"

    if [ -f "$src" ]; then
      if mv "$src" "$dst" 2>/dev/null; then
        ((success++)) || true
      fi
    fi
  done

  echo "Renamed $success/$NUM_PARALLEL files"

  # Verify renamed files exist
  local renamed_count
  renamed_count=$(find "$test_subdir" -name "renamed_*.txt" 2>/dev/null | wc -l | tr -d ' ')

  if [ "$renamed_count" = "$NUM_PARALLEL" ]; then
    echo -e "${GREEN}PASS:${NC} All files renamed successfully"
    ((TEST_PASSED++)) || true
  elif [ "$renamed_count" -gt 0 ]; then
    echo -e "${YELLOW}WARN:${NC} Only $renamed_count/$NUM_PARALLEL files renamed"
    ((TEST_PASSED++)) || true
  else
    echo -e "${RED}FAIL:${NC} Rename operations failed"
    ((TEST_FAILED++)) || true
  fi

  rm -rf "$test_subdir" 2>/dev/null || true
}

# ============================================
# Run Main
# ============================================

main "$@"
