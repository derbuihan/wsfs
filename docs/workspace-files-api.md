# Databricks Workspace Files API Reference

APIs used by wsfs to interact with Databricks Workspace.

## Overview

| API | Method | Purpose |
|-----|--------|---------|
| /api/2.0/workspace-files/object-info | GET | Get file metadata and signed URL |
| /api/2.0/workspace-files/list-files | GET | List directory contents |
| /api/2.0/workspace-files/new-files | POST | Create file with signed URL upload |
| /api/2.0/workspace-files/import-file/{path} | POST | Import file (fallback) |
| /api/2.0/workspace/rename | POST | Rename file or directory |

## Size-based API Selection Strategy

wsfs uses size-based API selection (5MB threshold) for optimal performance:

### Read Strategy

| File Size | Primary API | Fallback | Reason |
|-----------|-------------|----------|--------|
| < 5MB | workspace.Export | - | 1 round trip, simple |
| >= 5MB | signed URL | workspace.Export | Direct cloud storage download |

### Write Strategy

| File Size | Primary API | Fallback | Reason |
|-----------|-------------|----------|--------|
| < 5MB | import-file | - | 1 round trip, simple |
| >= 5MB | new-files + signed URL | import-file | Direct cloud storage upload |

### Threshold Rationale
- SDK Import/Export has 10MB limit
- 5MB threshold provides safety margin
- Avoids signed URL overhead for small files

## SDK-based Operations (Reference Only)

These use the official Databricks Go SDK and are well-documented:

| SDK Method | Purpose | wsfs Usage |
|------------|---------|------------|
| `workspace.Export` | Export notebooks/files | Read notebooks (JUPYTER format) |
| `workspace.Import` | Import notebooks | Write notebooks |
| `workspace.Delete` | Delete files/directories | Unlink, Rmdir |
| `workspace.Mkdirs` | Create directories | Mkdir |

See: https://pkg.go.dev/github.com/databricks/databricks-sdk-go/service/workspace

---

## Direct API Calls

### GET /api/2.0/workspace-files/object-info

Get file/directory metadata including signed URL for downloads.

**Request**:
- Query parameter: `path` - Workspace path (URL encoded)

**curl example**:
```bash
curl -X GET \
  "https://${DATABRICKS_HOST}/api/2.0/workspace-files/object-info?path=%2FUsers%2Fuser%40example.com%2Ftest.txt" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}"
```

**Response**:
```json
{
  "wsfs_object_info": {
    "object_info": {
      "path": "/Users/user@example.com/test.txt",
      "object_type": "FILE",
      "size": 1234,
      "modified_at": 1234567890000
    },
    "signed_url": {
      "url": "https://storage.googleapis.com/...",
      "headers": {
        "x-goog-signature": "..."
      }
    }
  }
}
```

**Response Fields**:
| Field | Type | Description |
|-------|------|-------------|
| `object_info.path` | string | Full workspace path |
| `object_info.object_type` | string | `FILE`, `DIRECTORY`, `NOTEBOOK`, `REPO` |
| `object_info.size` | int64 | File size in bytes |
| `object_info.modified_at` | int64 | Modification time (Unix milliseconds) |
| `signed_url.url` | string | Pre-signed URL for download |
| `signed_url.headers` | object | Headers required for signed URL request |
| `signed_url.url_type` | string | Cloud provider type: `AzureSasUri`, `GcsSignedUrl`, etc. |
| `signed_url.expiry_time_millis` | int64 | URL expiration time (Unix milliseconds) |

**Notes**:
- `signed_url` is optional (not present for notebooks or directories)
- Signed URL expires in approximately 15 minutes
- Used by: `client.go::statInternal()`

---

### GET /api/2.0/workspace-files/list-files

List contents of a directory.

**Request**:
- Query parameter: `path` - Directory path (URL encoded)

**curl example**:
```bash
curl -X GET \
  "https://${DATABRICKS_HOST}/api/2.0/workspace-files/list-files?path=%2FUsers%2Fuser%40example.com" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}"
```

**Response**:
```json
{
  "objects": [
    {
      "object_info": {
        "path": "/Users/user@example.com/file.txt",
        "object_type": "FILE",
        "size": 100,
        "modified_at": 1234567890000
      },
      "signed_url": {
        "url": "https://...",
        "headers": {}
      }
    },
    {
      "object_info": {
        "path": "/Users/user@example.com/notebook",
        "object_type": "NOTEBOOK",
        "size": 0,
        "modified_at": 1234567890000
      }
    }
  ]
}
```

**Notes**:
- Returns signed URLs for each regular file
- Notebooks do not have signed URLs
- Used by: `client.go::ReadDir()`

---

### POST /api/2.0/workspace-files/new-files

Create a new file using signed URL upload (two-step process).

**Step 1: Get signed URL**

**Request body**:
```json
{
  "path": "/Users/user@example.com/new-file.txt",
  "content": "<base64-encoded-content>"
}
```

**curl example**:
```bash
curl -X POST \
  "https://${DATABRICKS_HOST}/api/2.0/workspace-files/new-files" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"path": "/Users/user@example.com/new-file.txt", "content": "SGVsbG8gV29ybGQ="}'
```

**Response**:
```json
{
  "signed_urls": [
    {
      "url": "https://storage.googleapis.com/...",
      "headers": {
        "x-goog-signature": "..."
      }
    }
  ]
}
```

**Step 2: Upload to signed URL**

```bash
curl -X PUT "<signed_url>" \
  -H "x-goog-signature: <value>" \
  --data-binary @file.txt
```

**Notes**:
- May return 403 in some Databricks environments
- wsfs falls back to `import-file` on failure
- Used by: `client.go::writeViaNewFiles()`

---

### POST /api/2.0/workspace-files/import-file/{path}

Import file content directly (most reliable method).

**Request**:
- URL path: URL-encoded file path (without leading `/`)
- Query parameter: `overwrite=true`
- Body: raw file content (binary)
- Content-Type: `application/octet-stream`

**curl example**:
```bash
curl -X POST \
  "https://${DATABRICKS_HOST}/api/2.0/workspace-files/import-file/Users%2Fuser%40example.com%2Ffile.txt?overwrite=true" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/octet-stream" \
  --data-binary @file.txt
```

**Notes**:
- Most reliable write method across all environments
- Final fallback in wsfs write chain: `new-files` -> `import-file`
- Used by: `client.go::Write()`

---

### POST /api/2.0/workspace/rename

Rename a file or directory.

**Request body**:
```json
{
  "source_path": "/Users/user@example.com/old-name.txt",
  "destination_path": "/Users/user@example.com/new-name.txt"
}
```

**curl example**:
```bash
curl -X POST \
  "https://${DATABRICKS_HOST}/api/2.0/workspace/rename" \
  -H "Authorization: Bearer ${DATABRICKS_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{"source_path": "/Users/user@example.com/old.txt", "destination_path": "/Users/user@example.com/new.txt"}'
```

**Notes**:
- Works for files, directories, and notebooks
- Used by: `client.go::Rename()`

---

## Signed URL Usage

Signed URLs vary by cloud provider:
- **Azure**: SAS URI with `x-ms-*` headers
- **GCS**: Pre-signed URL with `x-goog-*` headers
- **AWS**: Pre-signed URL with AWS signature headers

### Reading via Signed URL

After obtaining a signed URL from `object-info` or `list-files`:

```bash
# Azure example (headers from response)
curl -X GET "<signed_url>" \
  -H "x-ms-version: 2025-01-05" \
  -H "x-ms-encryption-algorithm: AES256" \
  -H "x-ms-encryption-key: <value>" \
  -H "x-ms-encryption-key-sha256: <value>"
```

### Writing via Signed URL

After obtaining a signed URL from `new-files`:

```bash
# Azure example
curl -X PUT "<signed_url>" \
  -H "x-ms-blob-type: BlockBlob" \
  -H "x-ms-version: 2025-01-05" \
  -H "x-ms-encryption-algorithm: AES256" \
  -H "x-ms-encryption-key: <value>" \
  -H "x-ms-encryption-key-sha256: <value>" \
  --data-binary @file.txt
```

**Important**: Always use ALL headers returned in `signed_url.headers` field. Missing headers will cause authentication failures.

---

## Error Handling

| HTTP Status | Meaning | wsfs Behavior |
|-------------|---------|---------------|
| 200 | Success | Continue |
| 403 | Forbidden / Feature not available | Fall back to next method |
| 404 | Not found | Return ENOENT |
| 429 | Rate limited | Retry with exponential backoff |
| 5xx | Server error | Retry with exponential backoff |

wsfs implements automatic retry with exponential backoff (1s -> 2s -> 4s -> 8s -> 16s) for transient errors.
