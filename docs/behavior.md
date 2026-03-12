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
  - chmod-style mode updates as compatibility no-ops
  - the initial post-create timestamp sync for a brand-new empty file as a compatibility no-op (`touch new-file`)
- Unsupported (`ENOTSUP`):
  - atime-only updates
  - mtime-only updates
  - combined atime+mtime updates such as `touch existing-file`
  - chown / chgrp
- `chmod` requests succeed but do not change reported mode bits or backend permissions.
- When truncate and timestamps are requested together, wsfs performs the size change and ignores the requested timestamps. The backend write time becomes the effective `mtime`.

## Cache semantics

wsfs always uses two cache layers:

- metadata cache for lookup / getattr / readdir-style operations
- disk-backed file-content cache for reads

Important details:

- Clean read-only `Open` reuses cached metadata while the metadata TTL is still fresh (`10s` by default).
- Once the metadata TTL expires, the next `Lookup` / `Getattr` / read-only `Open` rechecks remote metadata.
- Visible notebook source files materialize exact exported source size on metadata paths (`stat(2)` / `lookup` / first read-only `open`) when the size is not known yet, then keep reusing it while the notebook identity stays unchanged.
- If that metadata changed, wsfs:
  - drops any clean in-memory buffer
  - invalidates related disk-cache entries
  - avoids `KEEP_CACHE` for that open so the kernel does not serve stale file content
- Missing or checksum-mismatched disk-cache files are invalidated and re-fetched once before read/write fails.
- Local write, rename, delete, mkdir, and rmdir invalidate relevant metadata and content-cache state.

This behavior is designed to keep search/indexing throughput reasonable for VSCode and `rg` while accepting a short TTL-sized stale window for out-of-band remote changes.

## Search-heavy workloads

- Prefer mounting a narrow subtree with `--remote-path` instead of opening the whole workspace root in your editor.
- Exclude dependency, build, and cache directories in editor settings (`.git`, `node_modules`, `.venv`, `dist`, `build`, `target`, `__pycache__`, `.pytest_cache`).
- Expect out-of-band remote overwrites to become visible after the metadata TTL boundary rather than on every read-only reopen.

## Git-heavy workloads

- Direct `.git` on wsfs is correctness-supported for `git init`, `git status`, `git add`, and `git commit`.
- Prefer a local separate git dir so `.git` lives on a local filesystem while the working tree remains on wsfs.
- `scripts/tests/git_diagnostic.sh` is the quick way to compare cold/warm metadata timings on a mounted repo.
- Git's `untracked-cache` and `fsmonitor` can help as temporary mitigations, but they do not replace wsfs-side metadata optimizations.

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
