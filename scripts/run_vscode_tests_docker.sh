#!/bin/bash

# Docker-based VSCode integration tests for wsfs
# Usage: ./scripts/run_vscode_tests_docker.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

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
echo "wsfs VSCode Integration Tests (Docker)"
echo "========================================"
echo "Databricks Host: ${DATABRICKS_HOST}"
echo ""

docker compose run --rm wsfs-vscode-test bash -c "
  set -euo pipefail

  cleanup() {
    fusermount3 -u /mnt/wsfs || fusermount -u /mnt/wsfs || umount /mnt/wsfs || true
    if [ -n \"\${WSFS_PID:-}\" ]; then
      kill \"\${WSFS_PID}\" 2>/dev/null || true
    fi
  }
  trap cleanup EXIT

  # Build wsfs
  echo 'Building wsfs...'
  go build -o /tmp/wsfs ./cmd/wsfs

  # Set up directories
  mkdir -p /mnt/wsfs /tmp/wsfs-cache

  # Mount wsfs
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

  WORKSPACE_DIR=\"/mnt/wsfs/vscode_e2e_\$(date +%s)\"
  mkdir -p \"\${WORKSPACE_DIR}/.vscode\"
  cat > \"\${WORKSPACE_DIR}/.vscode/settings.json\" <<'SETTINGS'
{
  \"python.defaultInterpreterPath\": \"/usr/bin/python3\"
}
SETTINGS

  export VSCODE_E2E_WORKSPACE=\"\${WORKSPACE_DIR}\"
  export VSCODE_TEST_USER_DIR=\"/tmp/vscode-test/user-data\"
  export VSCODE_TEST_EXT_DIR=\"/tmp/vscode-test/extensions\"

  cd /work/vscode-tests
  npm install
  npm run build
  xvfb-run -a npm test
"
