# Workflow
1. Check `Task.md` before changing anything.
2. Keep changes small and focused.
3. Format and test when the change can affect behavior.
   - If you change Go files, run `gofmt -w` on the touched files first.
   - Start with the smallest relevant Go tests.
   - Prefer `go test ./...` before Docker/FUSE runs.
   - Run `./scripts/test_docker.sh --fuse-only` for FUSE, mount, or integration-facing changes.
   - Run `./scripts/test_vscode_docker.sh` when VSCode integration behavior or `scripts/tests/vscode/` changes.
   - If you change `scripts/tests/vscode/`, also run `cd scripts/tests/vscode && npm run build` for TypeScript checking.
4. Update `Task.md` for substantive progress.

# Common commands
- `./scripts/run_wsfs_docker.sh` — open a Docker shell with wsfs mounted at `/mnt/wsfs`
- `./scripts/run_wsfs_docker.sh --debug` — same shell with debug logging
- `./scripts/run_wsfs_docker.sh -- 'ls /mnt/wsfs'` — run one command inside the mounted container
- `./scripts/test_docker.sh` — run the standard Docker integration suites
- `./scripts/test_vscode_docker.sh` — run the VSCode Docker integration tests

# Notes
- Preserve established filesystem semantics; see `docs/behavior.md` when touching FUSE behavior.
- Required env vars: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
- Never commit `.env`.

See `README.md` for detailed workflow and troubleshooting.
