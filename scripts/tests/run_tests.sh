#!/bin/bash

# Test Runner for wsfs
# Runs all integration tests on a mounted wsfs filesystem
#
# Usage:
#   ./run_tests.sh /path/to/mountpoint [options]
#
# Options:
#   --cache-dir=PATH    Cache directory (default: /tmp/wsfs-cache)
#   --log-file=PATH     Log file (default: /tmp/wsfs-test.log)
#   --fuse-only         Run only FUSE tests
#   --cache-only        Run only cache tests
#   --skip-cache        Skip cache tests
#
# Example:
#   ./run_tests.sh /mnt/wsfs
#   ./run_tests.sh /mnt/wsfs --cache-dir=/tmp/my-cache --fuse-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

# Default values
MOUNT_POINT=""
CACHE_DIR="/tmp/wsfs-cache"
LOG_FILE="/tmp/wsfs-test.log"
RUN_FUSE=true
RUN_CACHE=true

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --cache-dir=*)
      CACHE_DIR="${1#*=}"
      shift
      ;;
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --fuse-only)
      RUN_CACHE=false
      shift
      ;;
    --cache-only)
      RUN_FUSE=false
      shift
      ;;
    --skip-cache)
      RUN_CACHE=false
      shift
      ;;
    -*)
      echo "Unknown option: $1"
      exit 1
      ;;
    *)
      MOUNT_POINT="$1"
      shift
      ;;
  esac
done

if [ -z "$MOUNT_POINT" ]; then
  echo "Usage: $0 /path/to/mountpoint [options]"
  echo ""
  echo "Options:"
  echo "  --cache-dir=PATH    Cache directory (default: /tmp/wsfs-cache)"
  echo "  --log-file=PATH     Log file (default: /tmp/wsfs-test.log)"
  echo "  --fuse-only         Run only FUSE tests"
  echo "  --cache-only        Run only cache tests"
  echo "  --skip-cache        Skip cache tests"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

echo "========================================"
echo "wsfs Integration Test Runner"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Cache directory: ${CACHE_DIR}"
echo "Log file: ${LOG_FILE}"
echo "Run FUSE tests: ${RUN_FUSE}"
echo "Run Cache tests: ${RUN_CACHE}"
echo "========================================"
echo ""

OVERALL_RESULT=0

# Run FUSE tests
if [ "$RUN_FUSE" = true ]; then
  echo "Running FUSE tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/fuse_test.sh" "$MOUNT_POINT"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

# Run Cache tests
if [ "$RUN_CACHE" = true ]; then
  echo "Running Cache tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/cache_test.sh" "$MOUNT_POINT" "$CACHE_DIR" "$LOG_FILE"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

echo "========================================"
if [ $OVERALL_RESULT -eq 0 ]; then
  echo -e "${GREEN}ALL TEST SUITES PASSED${NC}"
else
  echo -e "${RED}SOME TEST SUITES FAILED${NC}"
fi
echo "========================================"

exit $OVERALL_RESULT
