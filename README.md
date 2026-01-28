# Databricks Workspace File System (wsfs)

A FUSE-based file system to interact with Databricks workspace files and directories as if they were part of the local file system.

## Features

- [x] Mount Databricks workspace.
- [x] List files and directories.
- [x] Read files.
- [x] Write files.
- [x] Make files and directories.
- [x] Delete files and directories.
- [x] Support for filesystem operations (`Rename`, `Fsync` and `Setattr`).
- [ ] Cache files for faster access. (in progress)

Notes:
- `Setattr` currently supports size changes (truncate) and mtime updates. Permissions/ownership changes are ignored.
- Vim saves are verified in the test suite.

## Distribution & Development Experience

- [ ] Automate release builds using GitHub Actions.
- [ ] Support installation via Homebrew (`brew install`).
- [ ] Expand unit and integration tests to ensure stability.
- [ ] Allow users to develop on Databricks directly from VSCode by running wsfs within a Remote Container.

## Usage

1. Install FUSE on your system if you haven't already.
2. Set the `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables with your Databricks workspace URL and personal access token.

```bash
$ cat .env
export DATABRICKS_HOST=<your-databricks-workspace-url>
export DATABRICKS_TOKEN=<your-personal-access-token>
```

3. Run the application with the desired mount point.

```bash
$ source .env
$ go build -o wsfs
$ ./wsfs <mount-point>
```

4. Access your Databricks workspace files through the mount point.

```bash
$ cd <mount-point>
$ ls
Repos  Shared  Users
```

## Testing

### Docker (recommended on macOS)

This uses `docker-compose.yml` and runs the test script (`scripts/fuse_test.sh`) inside a privileged container with FUSE enabled. The Docker image includes Vim so the save-path tests run in CI.

```bash
$ docker compose run --rm --build wsfs-test
```

Notes:
- The container reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from `.env` via `env_file`.
- `.env` is intentionally excluded from Git. Do not commit secrets.

### Linux (direct)

If you want to run tests directly on Linux without Docker:

```bash
$ sudo apt-get update
$ sudo apt-get install -y fuse3
$ sudo apt-get install -y vim
$ echo 'user_allow_other' | sudo tee -a /etc/fuse.conf
$ mkdir -p /mnt/wsfs
$ go build -o tmp/wsfs
$ ./tmp/wsfs /mnt/wsfs &
$ ./scripts/fuse_test.sh /mnt/wsfs
$ fusermount3 -u /mnt/wsfs
```

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
