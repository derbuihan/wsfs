#!/bin/bash

# ripgrep workload diagnostic for a mounted wsfs filesystem.
# Prints cold/warm timings and recent debug-log excerpts without failing on timing.
#
# Usage: ./rg_diagnostic.sh /path/to/mountpoint [log_file]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

MOUNT_POINT="${1:-}"
LOG_FILE="${2:-/tmp/wsfs-test.log}"

if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint [log_file]"
  exit 1
fi

if ! command -v rg >/dev/null 2>&1; then
  skip_test "rg is not installed; skipping diagnostic"
  exit 0
fi

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

run_rg_pass() {
  local label="$1"
  local start end elapsed output

  start="$(now_ms)"
  output="$(rg -n --hidden \
    --glob '!**/.git/**' \
    --glob '!**/node_modules/**' \
    --glob '!**/.venv/**' \
    --glob '!**/dist/**' \
    --glob '!**/build/**' \
    --glob '!**/target/**' \
    --glob '!**/__pycache__/**' \
    --glob '!**/.pytest_cache/**' \
    "$SEARCH_TOKEN" .)"
  end="$(now_ms)"
  elapsed=$((end - start))

  echo "${label} elapsed: ${elapsed}ms"
  assert_contains "$output" "src/main.py" "${label} finds src/main.py"
  assert_contains "$output" "pkg/helper.ts" "${label} finds pkg/helper.ts"

  if echo "$output" | grep -q "node_modules"; then
    echo -e "${RED}✗ FAIL:${NC} ${label} should not include excluded node_modules results"
    ((TEST_FAILED++)) || true
  else
    echo -e "${GREEN}✓ PASS:${NC} ${label} excludes node_modules hits"
    ((TEST_PASSED++)) || true
  fi
}

setup_test_dir "$MOUNT_POINT" "rg_diagnostic"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

print_section "ripgrep Diagnostic"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Log file: ${LOG_FILE}"

SEARCH_TOKEN="wsfs-rg-diagnostic-hit"
mkdir -p src pkg node_modules/pkg dist .git
printf "print('%s')\n" "$SEARCH_TOKEN" > src/main.py
printf "export const token = '%s';\n" "$SEARCH_TOKEN" > pkg/helper.ts
printf "module.exports = '%s';\n" "$SEARCH_TOKEN" > node_modules/pkg/index.js
printf "console.log('%s');\n" "$SEARCH_TOKEN" > dist/bundle.js
printf 'ref: refs/heads/main\n' > .git/HEAD

run_rg_pass "Cold search"
run_rg_pass "Warm search"

if [ -f "$LOG_FILE" ]; then
  echo ""
  echo "Recent log excerpt:"
  tail -n 40 "$LOG_FILE" || true
fi

print_test_summary
