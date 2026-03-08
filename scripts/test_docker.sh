#!/bin/bash

# Docker-based integration test runner for wsfs.
# Uses scripts/run_wsfs_docker.sh as the shared mount/build/cleanup wrapper.
#
# Usage:
#   ./scripts/test_docker.sh [options]
#
# Options:
#   --build           Rebuild the Docker image before testing
#   --fuse-only       Run only FUSE tests
#   --cache-only      Run only cache tests
#   --stress-only     Run only stress tests
#   --security-only   Run only security tests under --allow-other
#   --skip-cache      Skip cache tests
#   --skip-stress     Skip stress tests
#   --skip-security   Skip security tests
#   --log-file=PATH   Override log file for mounted runner
#   --help            Show this help

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_WSFS_SCRIPT="${SCRIPT_DIR}/run_wsfs_docker.sh"

cd "$ROOT_DIR"

DO_BUILD=false
RUN_MAIN=true
RUN_SECURITY=true
BUILT_IMAGE=false
LOG_FILE="/tmp/wsfs.log"
FORWARDED_ARGS=()

usage() {
  awk 'NR == 1 { next } !started && /^$/ { next } /^#/ { started = 1; sub(/^# ?/, ""); print; next } started { exit }' "$0"
}

quote_command() {
  local quoted
  printf -v quoted '%q ' "$@"
  printf '%s' "${quoted% }"
}

run_in_wsfs_container() {
  local allow_other="$1"
  shift

  local command_string
  command_string="$(quote_command "$@")"

  local args=(
    "$RUN_WSFS_SCRIPT"
    "--service=wsfs-test"
    "--workdir=/work"
    "--log-file=${LOG_FILE}"
    "--debug"
  )

  if [ "$DO_BUILD" = true ] && [ "$BUILT_IMAGE" = false ]; then
    args+=("--build")
    BUILT_IMAGE=true
  fi

  if [ "$allow_other" = true ]; then
    args+=("--allow-other")
  fi

  args+=("--" "$command_string")
  "${args[@]}"
}

run_main_stage() {
  local command=(
    "./scripts/tests/run.sh"
    "/mnt/wsfs"
    "--log-file=${LOG_FILE}"
    "--skip-security"
    "${FORWARDED_ARGS[@]}"
  )

  echo "========================================"
  echo "Stage 1: Main Test Suite"
  echo "========================================"
  run_in_wsfs_container false "${command[@]}"
}

run_security_stage() {
  local command=(
    "./scripts/tests/run.sh"
    "/mnt/wsfs"
    "--security-only"
  )

  echo ""
  echo "========================================"
  echo "Stage 2: Allow-Other Exposure Tests"
  echo "========================================"
  run_in_wsfs_container true "${command[@]}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD=true
      shift
      ;;
    --security-only)
      RUN_MAIN=false
      RUN_SECURITY=true
      shift
      ;;
    --skip-security)
      RUN_SECURITY=false
      shift
      ;;
    --fuse-only|--cache-only|--stress-only)
      RUN_MAIN=true
      RUN_SECURITY=false
      FORWARDED_ARGS+=("$1")
      shift
      ;;
    --skip-cache|--skip-stress)
      FORWARDED_ARGS+=("$1")
      shift
      ;;
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      FORWARDED_ARGS+=("$1")
      shift
      ;;
  esac
done

echo "========================================"
echo "wsfs Docker Test Runner"
echo "========================================"
echo "Run main tests: ${RUN_MAIN}"
echo "Run security tests: ${RUN_SECURITY}"
echo "Log file: ${LOG_FILE}"
echo "Forwarded options: ${FORWARDED_ARGS[*]:-<none>}"
echo ""

OVERALL_RESULT=0

if [ "$RUN_MAIN" = true ]; then
  run_main_stage || OVERALL_RESULT=1
fi

if [ "$RUN_SECURITY" = true ]; then
  run_security_stage || OVERALL_RESULT=1
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
