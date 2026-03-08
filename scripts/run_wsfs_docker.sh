#!/bin/bash

# Run wsfs inside Docker and open a shell (or command) in the mounted workspace.
# This keeps the mount inside the container, which works consistently on macOS and Linux.
#
# Usage:
#   ./scripts/run_wsfs_docker.sh [options] [-- command]
#
# Options:
#   --build                 Rebuild the selected Docker service image before starting
#   --debug                 Start wsfs with --debug
#   --allow-other           Start wsfs with --allow-other inside the container
#   --service=NAME          Docker Compose service to run (default: wsfs-test)
#   --log-file=PATH         wsfs log file inside the container (default: /tmp/wsfs.log)
#   --remote-path=PATH      Remote Databricks path to mount
#   --mount-point=PATH      Mount point inside the container (default: /mnt/wsfs)
#   --workdir=PATH          Working directory inside the container after mount (default: mount point)
#   --help                  Show this help
#
# Examples:
#   ./scripts/run_wsfs_docker.sh
#   ./scripts/run_wsfs_docker.sh --debug
#   ./scripts/run_wsfs_docker.sh --remote-path=/Users/alice
#   ./scripts/run_wsfs_docker.sh --workdir=/work -- 'pwd && ./scripts/tests/run.sh /mnt/wsfs --fuse-only'
#   ./scripts/run_wsfs_docker.sh --service=wsfs-vscode-test --workdir=/work -- './scripts/tests/vscode/run_in_container.sh /mnt/wsfs'

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$ROOT_DIR"

DO_BUILD=false
DEBUG=false
ALLOW_OTHER=false
SERVICE="wsfs-test"
LOG_FILE="/tmp/wsfs.log"
REMOTE_PATH=""
MOUNT_POINT="/mnt/wsfs"
WORKDIR=""
CONTAINER_COMMAND=""

usage() {
  awk 'NR == 1 { next } !started && /^$/ { next } /^#/ { started = 1; sub(/^# ?/, ""); print; next } started { exit }' "$0"
}

load_env() {
  if [ ! -f .env ]; then
    echo "Error: .env file not found" >&2
    echo "Create .env with DATABRICKS_HOST and DATABRICKS_TOKEN" >&2
    exit 1
  fi

  set -a
  source .env
  set +a

  if [ -z "${DATABRICKS_HOST:-}" ] || [ -z "${DATABRICKS_TOKEN:-}" ]; then
    echo "Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set in .env" >&2
    exit 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      DO_BUILD=true
      shift
      ;;
    --debug)
      DEBUG=true
      shift
      ;;
    --allow-other)
      ALLOW_OTHER=true
      shift
      ;;
    --service=*)
      SERVICE="${1#*=}"
      shift
      ;;
    --log-file=*)
      LOG_FILE="${1#*=}"
      shift
      ;;
    --remote-path=*)
      REMOTE_PATH="${1#*=}"
      shift
      ;;
    --mount-point=*)
      MOUNT_POINT="${1#*=}"
      shift
      ;;
    --workdir=*)
      WORKDIR="${1#*=}"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      if [[ $# -gt 0 ]]; then
        CONTAINER_COMMAND="$*"
      fi
      break
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo >&2
      usage >&2
      exit 1
      ;;
  esac
done

load_env

if [ -z "$WORKDIR" ]; then
  WORKDIR="$MOUNT_POINT"
fi

if [ "$DO_BUILD" = true ]; then
  echo "Building Docker image for service: ${SERVICE}"
  docker compose build "$SERVICE"
  echo
fi

DOCKER_ENV=(
  -e "WSFS_MOUNT_POINT=${MOUNT_POINT}"
  -e "WSFS_REMOTE_PATH=${REMOTE_PATH}"
  -e "WSFS_CONTAINER_COMMAND=${CONTAINER_COMMAND}"
  -e "WSFS_WORKDIR=${WORKDIR}"
  -e "WSFS_LOG_FILE=${LOG_FILE}"
)

if [ "$DEBUG" = true ]; then
  DOCKER_ENV+=(-e "WSFS_DEBUG=1")
else
  DOCKER_ENV+=(-e "WSFS_DEBUG=0")
fi

if [ "$ALLOW_OTHER" = true ]; then
  DOCKER_ENV+=(-e "WSFS_ALLOW_OTHER=1")
else
  DOCKER_ENV+=(-e "WSFS_ALLOW_OTHER=0")
fi

echo "========================================"
echo "wsfs Docker Shell"
echo "========================================"
echo "Databricks Host: ${DATABRICKS_HOST}"
echo "Service: ${SERVICE}"
echo "Mount point (container): ${MOUNT_POINT}"
echo "Workdir (container): ${WORKDIR}"
echo "Log file (container): ${LOG_FILE}"
echo "Remote path: ${REMOTE_PATH:-/}"
echo "Debug: ${DEBUG}"
echo "Allow other: ${ALLOW_OTHER}"
if [ -n "$CONTAINER_COMMAND" ]; then
  echo "Command: ${CONTAINER_COMMAND}"
else
  echo "Command: interactive shell"
fi
echo "========================================"

docker compose run --rm "${DOCKER_ENV[@]}" "$SERVICE" bash -lc '
  set -euo pipefail

  cleanup() {
    fusermount3 -u "$WSFS_MOUNT_POINT" >/dev/null 2>&1 || \
      fusermount -u "$WSFS_MOUNT_POINT" >/dev/null 2>&1 || \
      umount "$WSFS_MOUNT_POINT" >/dev/null 2>&1 || true
    if [ -n "${WSFS_PID:-}" ]; then
      kill "$WSFS_PID" 2>/dev/null || true
    fi
  }
  trap cleanup EXIT

  echo "Building wsfs..."
  /usr/local/go/bin/go build -buildvcs=false -o /tmp/wsfs ./cmd/wsfs

  export XDG_CACHE_HOME=/tmp/xdg-cache
  CACHE_DIR="${XDG_CACHE_HOME}/wsfs"
  mkdir -p "$WSFS_MOUNT_POINT" "$CACHE_DIR" "$(dirname "$WSFS_LOG_FILE")"

  WSFS_ARGS=()
  if [ "${WSFS_DEBUG:-0}" = "1" ]; then
    WSFS_ARGS+=(--debug)
  fi
  if [ "${WSFS_ALLOW_OTHER:-0}" = "1" ]; then
    WSFS_ARGS+=(--allow-other)
  fi
  if [ -n "${WSFS_REMOTE_PATH:-}" ]; then
    WSFS_ARGS+=("--remote-path=${WSFS_REMOTE_PATH}")
  fi
  WSFS_ARGS+=("$WSFS_MOUNT_POINT")

  echo "Mounting wsfs..."
  echo "Using cache directory: $CACHE_DIR"
  /tmp/wsfs "${WSFS_ARGS[@]}" > "$WSFS_LOG_FILE" 2>&1 &
  WSFS_PID=$!

  for i in $(seq 1 30); do
    if grep -q " $WSFS_MOUNT_POINT " /proc/mounts 2>/dev/null; then
      break
    fi
    sleep 1
  done

  if ! grep -q " $WSFS_MOUNT_POINT " /proc/mounts 2>/dev/null; then
    echo "Mount failed"
    cat "$WSFS_LOG_FILE"
    exit 1
  fi

  echo "wsfs mounted successfully"
  echo "Mount is available inside the container at: $WSFS_MOUNT_POINT"
  echo

  cd "${WSFS_WORKDIR:-$WSFS_MOUNT_POINT}"

  if [ -n "${WSFS_CONTAINER_COMMAND:-}" ]; then
    exec bash -lc "$WSFS_CONTAINER_COMMAND"
  fi

  exec bash -i
'
