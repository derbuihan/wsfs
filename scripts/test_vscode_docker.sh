#!/bin/bash

# Docker-based VSCode integration tests for wsfs.
# Uses scripts/run_wsfs_docker.sh as the shared mount/build/cleanup wrapper.
#
# Usage:
#   ./scripts/test_vscode_docker.sh [--build]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_WSFS_SCRIPT="${SCRIPT_DIR}/run_wsfs_docker.sh"

cd "$ROOT_DIR"

DO_BUILD=false

usage() {
  awk 'NR == 1 { next } !started && /^$/ { next } /^#/ { started = 1; sub(/^# ?/, ""); print; next } started { exit }' "$0"
}

quote_command() {
  local quoted
  printf -v quoted '%q ' "$@"
  printf '%s' "${quoted% }"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD=true
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

COMMAND_STRING="$(quote_command ./scripts/tests/vscode/run_in_container.sh /mnt/wsfs)"
ARGS=(
  "$RUN_WSFS_SCRIPT"
  "--service=wsfs-vscode-test"
  "--workdir=/work"
  "--debug"
)

if [ "$DO_BUILD" = true ]; then
  ARGS+=("--build")
fi

ARGS+=("--" "$COMMAND_STRING")

"${ARGS[@]}"
