# wsfs behavior

This document collects the current runtime behavior that matters for editors,
POSIX expectations, and operational safety.

## Access semantics

- Without `--allow-other`, the mount is effectively owner-only.
  - The kernel limits access to the mounting user.
  - `Access()` also enforces the mount owner's UID inside wsfs.
- With `--allow-other`, wsfs does **not** apply per-user filtering.
  - Other local users can read, write, rename, and delete through the mount.
  - All operations still execute with the Databricks token owner's backend permissions.
- This is why wsfs is recommended for single-user development machines and not shared hosts.

## Attribute representation

- `stat(2)` ownership is synthetic but stable.
  - Files and directories report the mount owner's `uid/gid`.
- Mode bits are synthetic.
  - Regular files appear as `0644`-style entries.
  - Directories appear as `0755`-style entries.
- `Statfs` returns fixed synthetic values so common tools and editors continue to work.

## Supported and unsupported setattr operations

- Supported:
  - size changes / truncate
- Unsupported (`ENOTSUP`):
  - atime-only updates
  - mtime-only updates
  - combined atime+mtime updates such as `touch existing-file`
  - chmod / chown / chgrp
- When truncate and timestamps are requested together, wsfs performs the size change and ignores the requested timestamps. The backend write time becomes the effective `mtime`.

## Cache semantics

wsfs always uses two cache layers:

- metadata cache for lookup / getattr / readdir-style operations
- disk-backed file-content cache for reads

Important details:

- Clean read-only `Open` forces a remote metadata recheck even if the metadata TTL has not expired yet.
- If that metadata changed, wsfs:
  - drops any clean in-memory buffer
  - invalidates related disk-cache entries
  - avoids `KEEP_CACHE` for that open so the kernel does not serve stale file content
- Missing or checksum-mismatched disk-cache files are invalidated and re-fetched once before read/write fails.
- Local write, rename, delete, mkdir, and rmdir invalidate relevant metadata and content-cache state.

This behavior is designed to make editor reopen/save loops more reliable, especially in VSCode.

## Notebook source view

- Databricks notebooks are exposed as source files by language:
  - `.py`, `.sql`, `.scala`, `.R`
- If the preferred source filename collides with a real workspace entry, wsfs falls back to `.ipynb`.
- Creating `foo.py` creates a Python notebook named `foo` in Databricks.
- Creating `foo.ipynb` creates a regular workspace file named `foo.ipynb`.
- Rename operations keep notebook/source presentation consistent and refresh inode metadata after language-changing renames.

## Dirty-buffer behavior

- Dirty buffers stay authoritative for `Lookup` and `Getattr` so editors do not observe transient size regressions during save flows.
- `Flush`, `Fsync`, and last-handle `Release` push buffered writes back to Databricks.
- Dirty regular-file renames are flushed before the backend rename is attempted.
