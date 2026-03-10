# Databricks `projects` API investigation

This document summarizes the confirmed behavior of the hidden
PAT-accessible REST surface around `/api/2.0/projects/{id}`.

The public lifecycle API for Databricks Git folders is still `Repos`
(`GET/POST/PATCH /api/2.0/repos...`). Additional repo-related behavior is
available under `/api/2.0/projects` and nearby endpoints.

This is intentionally narrow:

- it covers the hidden REST surface only
- it does not document `ajax-api`
- it does not assume any unverified request schema or mutation behavior

## Overview

Confirmed relationship:

| Surface | Status | Notes |
|--------|--------|-------|
| `/api/2.0/repos` | Public | Public lifecycle API for listing, creating, and updating Git folders |
| `/api/2.0/projects` | Hidden REST | PAT-accessible hidden create route |
| `/api/2.0/projects/{id}` | Hidden REST | PAT-accessible repo metadata and some write-capable routes |
| `/api/2.0/projects/fetch-and-checkout` | Hidden REST | Path-based checkout/update operation |
| `/api/2.0/repos/{id}/branch-metadata` | Hidden REST | Adjacent PAT-accessible repo branch state |

`Repos` remains the documented entrypoint, while `projects` exposes
additional repo-oriented behavior that is not covered in the existing public
docs. Both path-based and id-based hidden operations are available.

## Confirmed read endpoints

### `GET /api/2.0/projects/{id}`

**Purpose**:
- returns repo metadata that overlaps with, but is richer than, the public
  `Repos` view

**Response shape**:

```json
{
  "project": {
    "id": 1234567890123456,
    "path": "/Users/<user>/<repo>",
    "git_url": "https://github.com/<org>/<repo>",
    "git_branch": "main",
    "git_head": "<commit-sha>",
    "git_provider": "gitHub",
    "workspace_filesystem_enabled": true,
    "git_state": "GIT_STATE_NORMAL"
  }
}
```

### `GET /api/2.0/repos/{id}/branch-metadata`

**Purpose**:
- returns branch divergence metadata relative to the remote

**Response shape**:

```json
{
  "commits_behind_remote_count": 0,
  "is_behind_remote": false,
  "commits_ahead_of_remote_count": 0
}
```

## Confirmed write-capable endpoints

The following routes were confirmed to exist as write-capable REST endpoints.
This document does **not** claim that all mutation semantics are fully known,
and it does **not** recommend using these routes in production code.

| Method | Endpoint | Confirmed result |
|--------|----------|----------------------------|
| `POST` | `/api/2.0/projects` | Successful creation of temporary Git folders |
| `DELETE` | `/api/2.0/projects/{id}` | Successful deletion of temporary Git folders |
| `POST` | `/api/2.0/projects/{id}/update` | Route accepted the request and returned a project object with the successful request shape |
| `POST` | `/api/2.0/projects/{id}/clone` | Route accepted schema-valid input and returned repo-specific state |
| `POST` | `/api/2.0/projects/fetch-and-checkout` | Successful path-based checkout/update |
| `POST` | `/api/2.0/projects/{id}/fetch-and-checkout` | Successful id-based checkout/update |

## Request and response behavior

### `POST /api/2.0/projects`

Behavior:

- `path` is required
- basic creation succeeded without an explicit `branch`, which implies the
  repository default branch is sufficient for the tested case
- creation succeeded with both of these request shapes:
  - `{"path":"/Users/<user>/<repo>","url":"https://github.com/<org>/<repo>","provider":"gitHub"}`
  - `{"path":"/Users/<user>/<repo>","git_url":"https://github.com/<org>/<repo>","git_provider":"gitHub"}`
- a missing parent path returns:
  `INVALID_PARAMETER_VALUE: Cannot create a Git folder (Repo) because the parent path does not exist.`

**Successful response shape**:

```json
{
  "project": {
    "id": 1234567890123456,
    "path": "/Users/<user>/<repo>",
    "git_url": "https://github.com/<org>/<repo>",
    "git_branch": "main",
    "git_head": "<commit-sha>",
    "git_provider": "gitHub",
    "workspace_filesystem_enabled": true,
    "git_state": "GIT_STATE_NORMAL"
  }
}
```

### `DELETE /api/2.0/projects/{id}`

Behavior:

- on a missing repo id:
  - response code: `404`
  - error code: `RESOURCE_DOES_NOT_EXIST`
  - message: `Git folder (Repo) could not be found`
- a successful deletion returns:
  - response code: `200`
  - response body: `{}`

This confirms that the route exists and performs repo lookup and deletion.

### `POST /api/2.0/projects/{id}/update`

Behavior:

- the route exists and is not a thin alias for public `Repos`
- the error message is misleadingly camelCase:
  `gitUrl and gitProvider needs to be set`
- successful requests used **snake_case**, not camelCase:
  - `{"git_url":"https://github.com/<org>/<repo>","git_provider":"gitHub"}` → `200`
- these variants returned `BAD_REQUEST` in the tested environment:
  - `{"gitUrl":"https://github.com/<org>/<repo>","gitProvider":"gitHub"}`
  - `{"url":"https://github.com/<org>/<repo>","provider":"gitHub"}`

**Successful response shape**:

```json
{
  "project": {
    "id": 1234567890123456,
    "path": "/Users/<user>/<repo>",
    "git_url": "https://github.com/<org>/<repo>",
    "git_branch": "main",
    "git_head": "<commit-sha>",
    "git_provider": "gitHub",
    "workspace_filesystem_enabled": true,
    "git_state": "GIT_STATE_NORMAL"
  }
}
```

The successful request reused the existing remote/provider of a temporary repo.
Changing the remote was not confirmed.

### `POST /api/2.0/projects/{id}/clone`

Behavior:

- schema-valid requests use snake_case:
  - `git_url`
  - `git_provider`
- on missing fields, the route returned field-specific validation errors such as:
  - `Missing required fields: git_provider, git_url`
  - `Missing required field: git_provider`
  - `Missing required field: git_url`
- on an already populated repo path, a schema-valid request returned:
  `INVALID_STATE: Failed to clone repo. Repo may be incomplete. Failure reason: Path already exists and not empty`

This confirms that the route exists, validates request fields, and performs
repo/path state checks.

### `POST /api/2.0/projects/fetch-and-checkout`

Behavior:

- `path` and `branch` are required
- validation errors were:
  - `Missing required fields: branch, path`
  - `Missing required field: branch`
  - `Missing required field: path`
- on an existing repo path and the current branch, the route returned `200`
  with this response:

```json
{
  "branch": "main"
}
```

A `GET` request to the same path returned `500 INTERNAL_ERROR`, so this should
not be treated as a read endpoint.

### `POST /api/2.0/projects/{id}/fetch-and-checkout`

Behavior:

- `branch` is required
- the route does not require `path` when a repo id is already present
- on an existing repo id and the current branch, the route returned `200` with
  this response:

```json
{
  "branch": "main",
  "previousBranch": "main"
}
```

### Remote access depends on Git credentials

Behavior:

- when `fetch-and-checkout` was asked to switch to a non-existent branch,
  both the path-based and id-based routes returned `401 UNAUTHENTICATED`
- the error reason can be `GIT_CREDENTIAL_MISSING`
- the message states that no Git credential is configured for the
  repository and suggests configuring Git integration in user settings

This is an important boundary: PAT authentication is sufficient to reach the
hidden REST surface, but remote Git access can still depend on Databricks-side
Git credentials.

### Mutation safety

Confirmed behavior:

- temporary repos created through `POST /api/2.0/projects` were visible through
  both `GET /api/2.0/projects/{id}` and `GET /api/2.0/repos/{id}`
- both temporary repos were later removed successfully through
  `DELETE /api/2.0/projects/{id}`
- earlier invalid request bodies against an existing repo did not change
  `git_head`, `git_branch`, or `git_state`

## `curl` examples

These examples use placeholders only.

### Read-only metadata lookup

```bash
curl -X GET \
  "https://<workspace-host>/api/2.0/projects/<repo-id>" \
  -H "Authorization: Bearer <token>"
```

### Read-only branch divergence lookup

```bash
curl -X GET \
  "https://<workspace-host>/api/2.0/repos/<repo-id>/branch-metadata" \
  -H "Authorization: Bearer <token>"
```

### Hidden project creation

```bash
curl -X POST \
  "https://<workspace-host>/api/2.0/projects" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"path":"/Users/<user>/<repo>","url":"https://github.com/<org>/<repo>","provider":"gitHub"}'
```

### Hidden project update

```bash
curl -X POST \
  "https://<workspace-host>/api/2.0/projects/<repo-id>/update" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"git_url":"https://github.com/<org>/<repo>","git_provider":"gitHub"}'
```

### Hidden path-based fetch-and-checkout

```bash
curl -X POST \
  "https://<workspace-host>/api/2.0/projects/fetch-and-checkout" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"path":"/Users/<user>/<repo>","branch":"main"}'
```

### Hidden id-based fetch-and-checkout

```bash
curl -X POST \
  "https://<workspace-host>/api/2.0/projects/<repo-id>/fetch-and-checkout" \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"branch":"main"}'
```

### Hidden project deletion

```bash
curl -X DELETE \
  "https://<workspace-host>/api/2.0/projects/<repo-id>" \
  -H "Authorization: Bearer <token>"
```

## Related notes

- `docs/workspace-files-api.md` documents the public and public-ish workspace
  file APIs already used by wsfs.
- This document is complementary: it records hidden repo/project REST behavior.
- This document does not cover `ajax-api`.
