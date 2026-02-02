#!/bin/bash

# Docker-based Test Runner for wsfs
# Thin wrapper that sets up Docker environment and calls run_tests.sh
#
# Usage:
#   ./run_tests_docker.sh [options]
#
# Options:
#   --build           Rebuild Docker image before testing
#   All other options are forwarded to run_tests.sh
#
# Examples:
#   ./run_tests_docker.sh                    # Run all tests
#   ./run_tests_docker.sh --fuse-only        # Run only FUSE tests
#   ./run_tests_docker.sh --security-only    # Run only security tests
#   ./run_tests_docker.sh --config-only      # Run only config tests
#   ./run_tests_docker.sh --build            # Rebuild and run all tests
#
# Requirements:
#   - Docker and docker-compose installed
#   - .env file with DATABRICKS_HOST and DATABRICKS_TOKEN

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

# Parse arguments
DO_BUILD=false
EXTRA_OPTS=""
RUN_SECURITY=true
RUN_CONFIG=true
RUN_MAIN=true

while [[ $# -gt 0 ]]; do
  case $1 in
    --build)
      DO_BUILD=true
      shift
      ;;
    --security-only)
      RUN_MAIN=false
      RUN_CONFIG=false
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
      ;;
    --config-only)
      RUN_MAIN=false
      RUN_SECURITY=false
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
      ;;
    --skip-security)
      RUN_SECURITY=false
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
      ;;
    --skip-config)
      RUN_CONFIG=false
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
      ;;
    --fuse-only|--cache-only|--stress-only)
      RUN_MAIN=true
      RUN_SECURITY=false
      RUN_CONFIG=false
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
      ;;
    *)
      EXTRA_OPTS="${EXTRA_OPTS} $1"
      shift
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
echo "Extra options: ${EXTRA_OPTS:-<none>}"
echo "Run main tests: ${RUN_MAIN}"
echo "Run security tests: ${RUN_SECURITY}"
echo "Run config tests: ${RUN_CONFIG}"
echo ""

# Build if requested
if [ "$DO_BUILD" = true ]; then
  echo "Building Docker image..."
  docker compose build wsfs-test
  echo ""
fi

# Common docker-compose run options
DOCKER_RUN="docker compose run --rm"

OVERALL_RESULT=0

# Stage 1: Run main test suite (FUSE, Cache, Stress)
if [ "$RUN_MAIN" = true ]; then
  echo "========================================"
  echo "Stage 1: Main Test Suite"
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

    # Run tests via run_tests.sh (skip security and config - they run separately)
    ./scripts/run_tests.sh /mnt/wsfs --cache-dir=/tmp/wsfs-cache --log-file=/tmp/wsfs.log --skip-security --skip-config ${EXTRA_OPTS}
    TEST_RESULT=\$?

    # Cleanup
    echo ''
    echo 'Cleaning up...'
    fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
    kill \$WSFS_PID 2>/dev/null || true

    exit \$TEST_RESULT
  " || OVERALL_RESULT=1
fi

# Stage 2: Run security tests (requires --allow-other mount)
if [ "$RUN_SECURITY" = true ]; then
  echo ""
  echo "========================================"
  echo "Stage 2: Security Tests (with --allow-other)"
  echo "========================================"

  $DOCKER_RUN wsfs-test bash -c "
    set -e

    # Build wsfs (if not already built)
    if [ ! -x /tmp/wsfs ]; then
      go build -o /tmp/wsfs ./cmd/wsfs
    fi

    # Set up directories
    mkdir -p /mnt/wsfs /tmp/wsfs-cache

    # Mount with --allow-other for security tests
    echo 'Mounting wsfs with --allow-other...'
    /tmp/wsfs --debug --allow-other --cache=true --cache-dir=/tmp/wsfs-cache /mnt/wsfs > /tmp/wsfs.log 2>&1 &
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

    echo 'wsfs mounted with --allow-other'
    echo ''

    # Run security tests only
    ./scripts/run_tests.sh /mnt/wsfs --security-only
    TEST_RESULT=\$?

    # Cleanup
    echo ''
    echo 'Cleaning up...'
    fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
    kill \$WSFS_PID 2>/dev/null || true

    exit \$TEST_RESULT
  " || OVERALL_RESULT=1
fi

# Stage 3: Run cache configuration tests (requires mount/unmount)
if [ "$RUN_CONFIG" = true ]; then
  echo ""
  echo "========================================"
  echo "Stage 3: Cache Configuration Tests"
  echo "========================================"

  $DOCKER_RUN wsfs-test bash -c "
    set -e

    # Build wsfs (if not already built)
    if [ ! -x /tmp/wsfs ]; then
      go build -o /tmp/wsfs ./cmd/wsfs
    fi

    # Run cache configuration tests
    ./scripts/run_tests.sh /mnt/wsfs --config-only --wsfs-binary=/tmp/wsfs --cache-dir=/tmp/wsfs-cache
  " || OVERALL_RESULT=1
fi

echo ""
echo "========================================"
if [ $OVERALL_RESULT -eq 0 ]; then
  echo "ALL DOCKER TESTS COMPLETED SUCCESSFULLY"
else
  echo "SOME DOCKER TESTS FAILED"
fi
echo "========================================"

exit $OVERALL_RESULT
