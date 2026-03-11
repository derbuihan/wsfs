#!/bin/bash

# Git workload diagnostic for a mounted wsfs filesystem.
# Prints cold/warm timings and recent debug-log excerpts without failing on timing.
#
# Usage:
#   ./git_diagnostic.sh /path/to/mountpoint [log_file] [--separate-git-dir /path/to/local.git]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

usage() {
  cat <<'USAGE'
Usage: ./git_diagnostic.sh /path/to/mountpoint [log_file] [--separate-git-dir /path/to/local.git]

Options:
  --separate-git-dir PATH   Keep Git metadata outside the mounted worktree
  --help                    Show this help
USAGE
}

now_ms() {
  if date +%s%3N >/dev/null 2>&1; then
    date +%s%3N
  else
    python3 - <<'PY'
import time
print(int(time.time() * 1000))
PY
  fi
}

run_git_pass() {
  local label="$1"
  shift

  local start end elapsed output status
  start="$(now_ms)"
  set +e
  output="$($@ 2>&1)"
  status=$?
  set -e
  end="$(now_ms)"
  elapsed=$((end - start))

  echo "${label} elapsed: ${elapsed}ms"
  if [ $status -ne 0 ]; then
    echo -e "${RED}✗ FAIL:${NC} ${label} exited with ${status}"
    echo "$output"
    ((TEST_FAILED++)) || true
    return 1
  fi

  echo -e "${GREEN}✓ PASS:${NC} ${label} exited successfully"
  ((TEST_PASSED++)) || true

  if [ -n "$output" ]; then
    echo "$output" | sed 's/^/  /'
  fi
}

MOUNT_POINT=""
LOG_FILE="/tmp/wsfs-test.log"
SEPARATE_GIT_DIR=""
TTL_WAIT_SECS="${WSFS_GIT_DIAGNOSTIC_TTL_WAIT:-11}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --separate-git-dir)
      if [ $# -lt 2 ]; then
        echo "Error: --separate-git-dir requires a path argument." >&2
        exit 1
      fi
      SEPARATE_GIT_DIR="$2"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --*)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      if [ -z "$MOUNT_POINT" ]; then
        MOUNT_POINT="$1"
      elif [ "$LOG_FILE" = "/tmp/wsfs-test.log" ]; then
        LOG_FILE="$1"
      else
        echo "Unexpected extra argument: $1" >&2
        usage >&2
        exit 1
      fi
      shift
      ;;
  esac
done

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  usage >&2
  exit 1
fi

if ! command -v git >/dev/null 2>&1; then
  skip_test "git is not installed; skipping diagnostic"
  exit 0
fi

setup_test_dir "$MOUNT_POINT" "git_diagnostic"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

REPO_DIR="$TEST_BASE_DIR/repo"
mkdir -p "$REPO_DIR"

if [ -n "$SEPARATE_GIT_DIR" ]; then
  mkdir -p "$(dirname "$SEPARATE_GIT_DIR")"
  git -C "$REPO_DIR" init --separate-git-dir "$SEPARATE_GIT_DIR" >/dev/null
else
  git -C "$REPO_DIR" init >/dev/null
fi

git -C "$REPO_DIR" config user.name "wsfs-diagnostic"
git -C "$REPO_DIR" config user.email "wsfs-diagnostic@example.com"

mkdir -p "$REPO_DIR/src" "$REPO_DIR/docs" "$REPO_DIR/config" "$REPO_DIR/data" "$REPO_DIR/scripts"

for i in $(seq 1 18); do
  printf '# Databricks notebook source\nprint("notebook-%02d")\n' "$i" > "$REPO_DIR/src/notebook_${i}.py"
done

printf '# wsfs git diagnostic\n' > "$REPO_DIR/README.md"
printf 'notes\n' > "$REPO_DIR/docs/notes.txt"
printf '{"setting":true}\n' > "$REPO_DIR/config/app.json"
printf 'name=value\n' > "$REPO_DIR/config/app.cfg"
printf 'id,value\n1,alpha\n' > "$REPO_DIR/data/sample.csv"
printf '#!/bin/sh\necho helper\n' > "$REPO_DIR/scripts/helper.sh"
printf 'plain text\n' > "$REPO_DIR/notes.txt"

git -C "$REPO_DIR" add .
git -C "$REPO_DIR" commit -m "initial diagnostic repo" >/dev/null

print_section "Git Diagnostic"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Repository: ${REPO_DIR}"
echo "Git dir: $(git -C "$REPO_DIR" rev-parse --git-dir)"
echo "TTL wait: ${TTL_WAIT_SECS}s"
echo "Log file: ${LOG_FILE}"

run_git_pass "Cold git status --short" git -C "$REPO_DIR" status --short
run_git_pass "Warm git status --short" git -C "$REPO_DIR" status --short
sleep "$TTL_WAIT_SECS"
run_git_pass "Post-TTL git status --short" git -C "$REPO_DIR" status --short
run_git_pass "git status --short --untracked-files=no" git -C "$REPO_DIR" status --short --untracked-files=no
run_git_pass "git rev-parse HEAD" git -C "$REPO_DIR" rev-parse HEAD
run_git_pass "git log -1" git -C "$REPO_DIR" log -1

if [ -f "$LOG_FILE" ]; then
  echo ""
  echo "Recent log excerpt:"
  tail -n 60 "$LOG_FILE" || true
fi

print_test_summary
