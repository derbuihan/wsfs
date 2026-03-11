#!/bin/bash

# Run the VSCode E2E suite inside a mounted wsfs Docker container.
# Usage: ./scripts/tests/vscode/run_in_container.sh [/path/to/mountpoint]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOUNT_POINT="${1:-/mnt/wsfs}"
WORKSPACE_DIR="${MOUNT_POINT}/vscode_e2e_$(date +%s)"

cleanup_workspace() {
  local original_status="$1"
  local cleanup_status=0
  local attempt=1
  local max_attempts=5

  echo ""
  echo "Cleaning up VSCode E2E workspace: ${WORKSPACE_DIR}"

  while [ "$attempt" -le "$max_attempts" ]; do
    cleanup_status=0
    rm -rf -- "${WORKSPACE_DIR}" || cleanup_status=$?

    if [ "$cleanup_status" -eq 0 ] && [ ! -e "${WORKSPACE_DIR}" ]; then
      sleep 1
      if [ ! -e "${WORKSPACE_DIR}" ]; then
        echo "Cleaned up VSCode E2E workspace: ${WORKSPACE_DIR}"
        return "$original_status"
      fi
    fi

    echo "Cleanup retry ${attempt}/${max_attempts} for VSCode E2E workspace: ${WORKSPACE_DIR}" >&2
    attempt=$((attempt + 1))
    sleep 1
  done

  echo "Failed to clean up VSCode E2E workspace: ${WORKSPACE_DIR}" >&2
  if [ "$original_status" -eq 0 ]; then
    return 1
  fi
  return "$original_status"
}

on_exit() {
  local status=$?
  cleanup_workspace "$status"
}

trap on_exit EXIT

if [ ! -d "$MOUNT_POINT" ]; then
  echo "Error: mount point not found: $MOUNT_POINT" >&2
  exit 1
fi

mkdir -p "${WORKSPACE_DIR}/.vscode"
cat > "${WORKSPACE_DIR}/.vscode/settings.json" <<'SETTINGS'
{
  "python.defaultInterpreterPath": "/usr/bin/python3",
  "search.exclude": {
    "**/.git": true,
    "**/node_modules": true,
    "**/.venv": true,
    "**/dist": true,
    "**/build": true,
    "**/target": true,
    "**/__pycache__": true,
    "**/.pytest_cache": true
  },
  "files.exclude": {
    "**/.git": true,
    "**/node_modules": true,
    "**/.venv": true,
    "**/dist": true,
    "**/build": true,
    "**/target": true,
    "**/__pycache__": true,
    "**/.pytest_cache": true
  },
  "files.watcherExclude": {
    "**/.git/**": true,
    "**/node_modules/**": true,
    "**/.venv/**": true,
    "**/dist/**": true,
    "**/build/**": true,
    "**/target/**": true,
    "**/__pycache__/**": true,
    "**/.pytest_cache/**": true
  }
}
SETTINGS

export VSCODE_E2E_WORKSPACE="$WORKSPACE_DIR"
export VSCODE_TEST_USER_DIR="/tmp/vscode-test/user-data"
export VSCODE_TEST_EXT_DIR="/tmp/vscode-test/extensions"
mkdir -p "$VSCODE_TEST_USER_DIR" "$VSCODE_TEST_EXT_DIR"

echo "========================================"
echo "wsfs VSCode Integration Tests (Docker)"
echo "========================================"
echo "Workspace: ${WORKSPACE_DIR}"
echo "Project: ${SCRIPT_DIR}"
echo ""

cd "$SCRIPT_DIR"
npm install
npm run build
xvfb-run -a npm test
