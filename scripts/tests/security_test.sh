#!/bin/bash

# Security test for wsfs mounts created with --allow-other.
# Verifies that the mount owner and other local users can both access the filesystem.
#
# Usage: ./security_test.sh /path/to/mountpoint

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/lib/test_helpers.sh"

MOUNT_POINT="${1:-}"
if [ -z "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: Missing mount point argument.${NC}"
  echo "Usage: $0 /path/to/mountpoint"
  exit 1
fi

if [ ! -d "$MOUNT_POINT" ]; then
  echo -e "${RED}Error: ${MOUNT_POINT} does not exist or is not a directory.${NC}"
  exit 1
fi

run_as_other() {
  if command -v sudo >/dev/null 2>&1; then
    sudo -n -u nobody -- "$@"
    return
  fi
  if [ "$(id -u)" -eq 0 ] && command -v runuser >/dev/null 2>&1; then
    runuser -u nobody -- "$@"
    return
  fi
  if [ "$(id -u)" -eq 0 ] && command -v su >/dev/null 2>&1; then
    local quoted
    printf -v quoted '%q ' "$@"
    su -s /bin/sh nobody -c "$quoted"
    return
  fi
  return 127
}

can_run_as_other=false
if id nobody >/dev/null 2>&1; then
  if run_as_other true >/dev/null 2>&1; then
    can_run_as_other=true
  fi
fi

setup_test_dir "$MOUNT_POINT" "security_test"
trap 'cleanup_test_dir "$TEST_BASE_DIR"' EXIT

echo "========================================"
echo "Security Test Suite (--allow-other exposure validation)"
echo "========================================"
echo "Mount point: ${MOUNT_POINT}"
echo "Test directory: ${TEST_BASE_DIR}"
echo "Running as: $(whoami) (UID: $(id -u))"
echo ""

print_section "Section 1: Owner Access"

assert 'ls "${TEST_BASE_DIR}" >/dev/null 2>&1' "Owner can list directory"

OWNER_FILE="${TEST_BASE_DIR}/owner_test_file.txt"
printf 'owner content' > "${OWNER_FILE}"
assert_file_exists "${OWNER_FILE}" "Owner can create file"
CONTENT=$(cat "${OWNER_FILE}")
assert_eq "owner content" "$CONTENT" "Owner can read file content"
printf 'owner modified' > "${OWNER_FILE}"
CONTENT=$(cat "${OWNER_FILE}")
assert_eq "owner modified" "$CONTENT" "Owner can modify file"
OWNER_DIR="${TEST_BASE_DIR}/owner_test_dir"
mkdir -p "${OWNER_DIR}"
assert_dir_exists "${OWNER_DIR}" "Owner can create directory"
rm "${OWNER_FILE}"
assert_not_exists "${OWNER_FILE}" "Owner can delete file"
rmdir "${OWNER_DIR}"
assert_not_exists "${OWNER_DIR}" "Owner can delete directory"

print_section "Section 2: Non-Owner Access Through --allow-other"

if [ "$can_run_as_other" != true ]; then
  skip_test "Cannot switch to user nobody in this environment"
else
  SHARED_FILE="${TEST_BASE_DIR}/shared.txt"
  SHARED_CONTENT='shared content'
  printf '%s' "$SHARED_CONTENT" > "${SHARED_FILE}"

  assert 'run_as_other ls "${TEST_BASE_DIR}" >/dev/null 2>&1' "Non-owner can list directory"

  OTHER_SIZE=$(run_as_other stat -c %s "${SHARED_FILE}")
  assert_eq "${#SHARED_CONTENT}" "$OTHER_SIZE" "Non-owner sees current file size after directory listing"

  OTHER_CONTENT=$(run_as_other cat "${SHARED_FILE}")
  assert_eq "$SHARED_CONTENT" "$OTHER_CONTENT" "Non-owner can read file"

  run_as_other sh -c "printf 'modified by nobody' > '${SHARED_FILE}'"
  CONTENT=$(cat "${SHARED_FILE}")
  assert_eq "modified by nobody" "$CONTENT" "Non-owner can modify file"

  OTHER_NEW_FILE="${TEST_BASE_DIR}/created_by_nobody.txt"
  run_as_other sh -c "printf 'created by nobody' > '${OTHER_NEW_FILE}'"
  assert_file_exists "${OTHER_NEW_FILE}" "Non-owner can create file"
  CONTENT=$(cat "${OTHER_NEW_FILE}")
  assert_eq "created by nobody" "$CONTENT" "Owner sees file created by non-owner"

  OTHER_DIR="${TEST_BASE_DIR}/dir_created_by_nobody"
  run_as_other mkdir "${OTHER_DIR}"
  assert_dir_exists "${OTHER_DIR}" "Non-owner can create directory"

  OTHER_DELETE_FILE="${TEST_BASE_DIR}/delete_me.txt"
  printf 'delete me' > "${OTHER_DELETE_FILE}"
  run_as_other rm "${OTHER_DELETE_FILE}"
  assert_not_exists "${OTHER_DELETE_FILE}" "Non-owner can delete file"

  OTHER_DELETE_DIR="${TEST_BASE_DIR}/delete_dir"
  mkdir "${OTHER_DELETE_DIR}"
  run_as_other rmdir "${OTHER_DELETE_DIR}"
  assert_not_exists "${OTHER_DELETE_DIR}" "Non-owner can delete directory"
fi

print_section "Section 3: Shared Mount Semantics"

TEAM_FILE="${TEST_BASE_DIR}/shared_visibility.txt"
printf 'owner view' > "${TEAM_FILE}"
if [ "$can_run_as_other" != true ]; then
  skip_test "Shared visibility check requires switching to another user"
else
  run_as_other sh -c "printf 'other user view' > '${TEAM_FILE}'"
  CONTENT=$(cat "${TEAM_FILE}")
  assert_eq "other user view" "$CONTENT" "Owner sees changes written through allow-other access"
fi
rm -f "${TEAM_FILE}" 2>/dev/null || true

print_test_summary
