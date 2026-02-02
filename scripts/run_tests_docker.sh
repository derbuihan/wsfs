#!/bin/bash

# Docker-based Test Runner for wsfs
# Use this on Mac or when you don't have FUSE available locally
#
# Usage:
#   ./run_tests_docker.sh [options]
#
# Options:
#   --build           Rebuild Docker image before testing
#   --fuse-only       Run only FUSE tests
#   --cache-only      Run only cache tests
#   --stress-only     Run only stress tests
#   --skip-cache      Skip cache tests
#   --skip-stress     Skip stress tests
#   --skip-config-test  Skip cache configuration tests (disabled mode, permissions, TTL)
#
# Requirements:
#   - Docker and docker-compose installed
#   - .env file with DATABRICKS_HOST and DATABRICKS_TOKEN

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

# Default options
DO_BUILD=false
RUN_TESTS_OPTS=""
FUSE_ONLY=false
SKIP_CONFIG_TEST=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --build)
      DO_BUILD=true
      shift
      ;;
    --fuse-only)
      FUSE_ONLY=true
      RUN_TESTS_OPTS="--fuse-only"
      shift
      ;;
    --cache-only)
      RUN_TESTS_OPTS="--cache-only"
      shift
      ;;
    --stress-only)
      RUN_TESTS_OPTS="--stress-only"
      shift
      ;;
    --skip-cache)
      RUN_TESTS_OPTS="${RUN_TESTS_OPTS} --skip-cache"
      shift
      ;;
    --skip-stress)
      RUN_TESTS_OPTS="${RUN_TESTS_OPTS} --skip-stress"
      shift
      ;;
    --skip-config-test)
      SKIP_CONFIG_TEST=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Check for .env file
if [ ! -f .env ]; then
  echo "Error: .env file not found"
  echo "Create .env with DATABRICKS_HOST and DATABRICKS_TOKEN"
  exit 1
fi

# Source .env to get credentials
set -a
source .env
set +a

if [ -z "${DATABRICKS_HOST:-}" ] || [ -z "${DATABRICKS_TOKEN:-}" ]; then
  echo "Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env"
  exit 1
fi

echo "========================================"
echo "wsfs Docker Test Runner"
echo "========================================"
echo "Databricks Host: ${DATABRICKS_HOST}"
echo "Test options: ${RUN_TESTS_OPTS:-<default: all tests>}"
echo ""

# Build if requested
if [ "$DO_BUILD" = true ]; then
  echo "Building Docker image..."
  docker compose build wsfs-test
  echo ""
fi

# Common docker-compose run options
DOCKER_RUN="docker compose run --rm"

# Run main test suite via run_tests.sh
echo "========================================"
echo "Running Main Test Suite"
echo "========================================"

$DOCKER_RUN wsfs-test bash -c "
  set -e

  # Build wsfs
  echo 'Building wsfs...'
  go build -o /tmp/wsfs ./cmd/wsfs

  # Set up directories
  mkdir -p /mnt/wsfs /tmp/wsfs-cache

  # Mount with cache enabled
  echo 'Mounting wsfs...'
  /tmp/wsfs --debug --cache=true --cache-dir=/tmp/wsfs-cache --cache-ttl=24h /mnt/wsfs > /tmp/wsfs.log 2>&1 &
  WSFS_PID=\$!

  # Wait for mount
  for i in \$(seq 1 30); do
    if grep -q ' /mnt/wsfs ' /proc/mounts 2>/dev/null; then
      break
    fi
    sleep 1
  done

  if ! grep -q ' /mnt/wsfs ' /proc/mounts; then
    echo 'Mount failed'
    cat /tmp/wsfs.log
    exit 1
  fi

  echo 'wsfs mounted successfully'
  echo ''

  # Run tests via run_tests.sh
  ./scripts/run_tests.sh /mnt/wsfs --cache-dir=/tmp/wsfs-cache --log-file=/tmp/wsfs.log ${RUN_TESTS_OPTS}
  TEST_RESULT=\$?

  # Cleanup
  echo ''
  echo 'Cleaning up...'
  fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
  kill \$WSFS_PID 2>/dev/null || true

  exit \$TEST_RESULT
"

# Run cache configuration tests (only if not --fuse-only and not --no-cache-test)
if [ "$FUSE_ONLY" = false ] && [ "$SKIP_CONFIG_TEST" = false ]; then
  echo ""
  echo "========================================"
  echo "Running Cache Configuration Tests"
  echo "========================================"

  $DOCKER_RUN wsfs-test bash -c '
    set -e

    # Build wsfs (if not already built)
    if [ ! -x /tmp/wsfs ]; then
      go build -o /tmp/wsfs ./cmd/wsfs
    fi

    # Run cache configuration tests
    ./scripts/tests/cache_config_test.sh /tmp/wsfs /mnt/wsfs /tmp/wsfs-cache
  '
fi

echo ""
echo "========================================"
echo "ALL DOCKER TESTS COMPLETED SUCCESSFULLY"
echo "========================================"
