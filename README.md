# Databricks Workspace File System (wsfs)

A FUSE-based file system to interact with Databricks workspace files and directories as if they were part of the local file system.

## Features

vim still doesn't work on mount points

- [x] Mount Databricks workspace.
- [x] List files and directories.
- [x] Read files.
- [x] Write files.
- [x] Make files and directories.
- [x] Delete files and directories.
- [ ] Support for filesystem operations (`Rename`, `Fsync` and `Setattr`).
- [ ] Cache files for faster access. (in progress)

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

## License

GPL-3.0 License. See the [LICENSE](LICENSE) file for details.
