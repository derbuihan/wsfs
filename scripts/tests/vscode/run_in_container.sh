#!/bin/bash

# Run the VSCode E2E suite inside a mounted wsfs Docker container.
# Usage: ./scripts/tests/vscode/run_in_container.sh [/path/to/mountpoint]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOUNT_POINT="${1:-/mnt/wsfs}"
WORKSPACE_DIR="${MOUNT_POINT}/vscode_e2e_$(date +%s)"

if [ ! -d "$MOUNT_POINT" ]; then
  echo "Error: mount point not found: $MOUNT_POINT" >&2
  exit 1
fi

mkdir -p "${WORKSPACE_DIR}/.vscode"
cat > "${WORKSPACE_DIR}/.vscode/settings.json" <<'SETTINGS'
{
  "python.defaultInterpreterPath": "/usr/bin/python3"
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
