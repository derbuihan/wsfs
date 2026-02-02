#!/bin/bash

# Test Runner for wsfs
# Runs all integration tests on a mounted wsfs filesystem
#
# Usage:
#   ./run_tests.sh /path/to/mountpoint [options]
#
# Options:
#   --cache-dir=PATH       Cache directory (default: /tmp/wsfs-cache)
#   --log-file=PATH        Log file (default: /tmp/wsfs-test.log)
#   --wsfs-binary=PATH     wsfs binary path (required for --config-only)
#   --fuse-only            Run only FUSE tests
#   --cache-only           Run only cache tests
#   --stress-only          Run only stress tests
#   --security-only        Run only security tests
#   --config-only          Run only cache configuration tests
#   --skip-cache           Skip cache tests
#   --skip-stress          Skip stress tests
#   --skip-security        Skip security tests
#   --skip-config          Skip cache configuration tests
#
# Example:
#   ./run_tests.sh /mnt/wsfs
#   ./run_tests.sh /mnt/wsfs --cache-dir=/tmp/my-cache --fuse-only
#   ./run_tests.sh /mnt/wsfs --security-only
#   ./run_tests.sh /mnt/wsfs --config-only --wsfs-binary=/tmp/wsfs

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/tests/lib/test_helpers.sh"

# Default values
MOUNT_POINT=""
CACHE_DIR="/tmp/wsfs-cache"
LOG_FILE="/tmp/wsfs-test.log"
WSFS_BINARY=""
RUN_FUSE=true
RUN_CACHE=true
RUN_STRESS=true
RUN_SECURITY=true
RUN_CONFIG=true

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
      RUN_STRESS=false
      RUN_SECURITY=false
      RUN_CONFIG=false
      shift
      ;;
    --cache-only)
      RUN_FUSE=false
      RUN_STRESS=false
      RUN_SECURITY=false
      RUN_CONFIG=false
      shift
      ;;
    --stress-only)
      RUN_FUSE=false
      RUN_CACHE=false
      RUN_SECURITY=false
      RUN_CONFIG=false
      shift
      ;;
    --security-only)
      RUN_FUSE=false
      RUN_CACHE=false
      RUN_STRESS=false
      RUN_CONFIG=false
      shift
      ;;
    --config-only)
      RUN_FUSE=false
      RUN_CACHE=false
      RUN_STRESS=false
      RUN_SECURITY=false
      shift
      ;;
    --wsfs-binary=*)
      WSFS_BINARY="${1#*=}"
      shift
      ;;
    --skip-cache)
      RUN_CACHE=false
      shift
      ;;
    --skip-stress)
      RUN_STRESS=false
      shift
      ;;
    --skip-security)
      RUN_SECURITY=false
      shift
      ;;
    --skip-config)
      RUN_CONFIG=false
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
  echo "  --cache-dir=PATH       Cache directory (default: /tmp/wsfs-cache)"
  echo "  --log-file=PATH        Log file (default: /tmp/wsfs-test.log)"
  echo "  --wsfs-binary=PATH     wsfs binary path (required for --config-only)"
  echo "  --fuse-only            Run only FUSE tests"
  echo "  --cache-only           Run only cache tests"
  echo "  --stress-only          Run only stress tests"
  echo "  --security-only        Run only security tests"
  echo "  --config-only          Run only cache configuration tests"
  echo "  --skip-cache           Skip cache tests"
  echo "  --skip-stress          Skip stress tests"
  echo "  --skip-security        Skip security tests"
  echo "  --skip-config          Skip cache configuration tests"
  exit 1
fi

# Config-only tests don't need the mount point to exist (they mount it themselves)
if [ "$RUN_CONFIG" = true ] && [ "$RUN_FUSE" = false ] && [ "$RUN_CACHE" = false ] && [ "$RUN_STRESS" = false ] && [ "$RUN_SECURITY" = false ]; then
  # Skip mount point check for config-only mode
  :
elif [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

echo "========================================"
echo "wsfs Integration Test Runner"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Cache directory: ${CACHE_DIR}"
echo "Log file: ${LOG_FILE}"
echo "wsfs binary: ${WSFS_BINARY:-<not specified>}"
echo "Run FUSE tests: ${RUN_FUSE}"
echo "Run Cache tests: ${RUN_CACHE}"
echo "Run Stress tests: ${RUN_STRESS}"
echo "Run Security tests: ${RUN_SECURITY}"
echo "Run Config tests: ${RUN_CONFIG}"
echo "========================================"
echo ""

OVERALL_RESULT=0

# Run FUSE tests
if [ "$RUN_FUSE" = true ]; then
  echo "Running FUSE tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/tests/fuse_test.sh" "$MOUNT_POINT"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

# Run Cache tests
if [ "$RUN_CACHE" = true ]; then
  echo "Running Cache tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/tests/cache_test.sh" "$MOUNT_POINT" "$CACHE_DIR" "$LOG_FILE"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

# Run Stress tests
if [ "$RUN_STRESS" = true ]; then
  echo "Running Stress tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/tests/stress_test.sh" "$MOUNT_POINT"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

# Run Security tests
if [ "$RUN_SECURITY" = true ]; then
  echo "Running Security tests..."
  echo ""
  if ! bash "${SCRIPT_DIR}/tests/security_test.sh" "$MOUNT_POINT"; then
    OVERALL_RESULT=1
  fi
  echo ""
fi

# Run Cache Configuration tests
if [ "$RUN_CONFIG" = true ]; then
  echo "Running Cache Configuration tests..."
  echo ""
  if [ -z "$WSFS_BINARY" ]; then
    echo -e "${YELLOW}Warning: --wsfs-binary not specified, skipping config tests${NC}"
  else
    if ! bash "${SCRIPT_DIR}/tests/cache_config_test.sh" "$WSFS_BINARY" "$MOUNT_POINT" "$CACHE_DIR"; then
      OVERALL_RESULT=1
    fi
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
