#!/bin/bash

# Mounted test runner for wsfs.
# Runs integration test suites against an already-mounted wsfs filesystem.
#
# Usage:
#   ./scripts/tests/run.sh /path/to/mountpoint [options]
#
# Cache directory:
#   Derived automatically from XDG_CACHE_HOME/wsfs or ~/.cache/wsfs.
#
# Options:
#   --log-file=PATH        Log file (default: /tmp/wsfs-test.log)
#   --fuse-only            Run only FUSE tests
#   --cache-only           Run only cache tests
#   --stress-only          Run only stress tests
#   --security-only        Run only security tests
#   --skip-cache           Skip cache tests
#   --skip-stress          Skip stress tests
#   --skip-security        Skip security tests
#   --help                 Show this help
#
# Examples:
#   ./scripts/tests/run.sh /mnt/wsfs
#   ./scripts/tests/run.sh /mnt/wsfs --fuse-only
#   ./scripts/tests/run.sh /mnt/wsfs --security-only

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

resolve_cache_dir() {
  if [ -n "${XDG_CACHE_HOME:-}" ]; then
    echo "${XDG_CACHE_HOME}/wsfs"
    return
  fi

  if [ -n "${HOME:-}" ]; then
    echo "${HOME}/.cache/wsfs"
    return
  fi

  echo "/tmp/wsfs-cache"
}

usage() {
  awk 'NR == 1 { next } !started && /^$/ { next } /^#/ { started = 1; sub(/^# ?/, ""); print; next } started { exit }' "$0"
}

run_suite() {
  local label="$1"
  shift

  echo "Running ${label}..."
  echo ""
  if ! bash "$@"; then
    OVERALL_RESULT=1
  fi
  echo ""
}

MOUNT_POINT=""
CACHE_DIR="$(resolve_cache_dir)"
LOG_FILE="/tmp/wsfs-test.log"
RUN_FUSE=true
RUN_CACHE=true
RUN_STRESS=true
RUN_SECURITY=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --fuse-only)
      RUN_CACHE=false
      RUN_STRESS=false
      RUN_SECURITY=false
      shift
      ;;
    --cache-only)
      RUN_FUSE=false
      RUN_STRESS=false
      RUN_SECURITY=false
      shift
      ;;
    --stress-only)
      RUN_FUSE=false
      RUN_CACHE=false
      RUN_SECURITY=false
      shift
      ;;
    --security-only)
      RUN_FUSE=false
      RUN_CACHE=false
      RUN_STRESS=false
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
    --help|-h)
      usage
      exit 0
      ;;
    -* )
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
  usage
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

echo "========================================"
echo "wsfs Mounted Test Runner"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Cache directory: ${CACHE_DIR}"
echo "Log file: ${LOG_FILE}"
echo "Run FUSE tests: ${RUN_FUSE}"
echo "Run Cache tests: ${RUN_CACHE}"
echo "Run Stress tests: ${RUN_STRESS}"
echo "Run Security tests: ${RUN_SECURITY}"
echo "========================================"
echo ""

OVERALL_RESULT=0

if [ "$RUN_FUSE" = true ]; then
  run_suite "FUSE tests" "${SCRIPT_DIR}/fuse_test.sh" "$MOUNT_POINT"
fi

if [ "$RUN_CACHE" = true ]; then
  run_suite "Cache tests" "${SCRIPT_DIR}/cache_test.sh" "$MOUNT_POINT" "$CACHE_DIR" "$LOG_FILE"
fi

if [ "$RUN_STRESS" = true ]; then
  run_suite "Stress tests" "${SCRIPT_DIR}/stress_test.sh" "$MOUNT_POINT"
fi

if [ "$RUN_SECURITY" = true ]; then
  run_suite "Security tests" "${SCRIPT_DIR}/security_test.sh" "$MOUNT_POINT"
fi

echo "========================================"
if [ $OVERALL_RESULT -eq 0 ]; then
  echo -e "${GREEN}ALL TEST SUITES PASSED${NC}"
else
  echo -e "${RED}SOME TEST SUITES FAILED${NC}"
fi
echo "========================================"

exit $OVERALL_RESULT
