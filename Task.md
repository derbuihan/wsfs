# Future Tasks (wsfs)

このファイルは今後のオプション機能と、未解決の課題だけを短くまとめる。

---

## 完了（2026-02-08）

- [x] systemd パッケージングの引数分割（WSFS_OPTS 廃止、env 変数分割、例更新）

---

## 完了（2026-02-07）

- [x] ファイル監視環境の書き込み競合を解消（truncateのflush遅延、dirty-aware Lookup/Getattr、openCount/dirtyFlags、OnForget保持、テスト）
- [x] Renameのサブツリー更新
- [x] サイズベース API 選択戦略（5MB）
- [x] docs/ 配下の整理
- [x] VSCode test-electron 統合試験（Core開発ループ、TS実装）
- [x] ユニットテストの拡充とカバレッジ改善（fuse/retry/databricks/filecache/logging/CLI）
- [x] リリース成果物のバージョン表記をコミットハッシュに変更

---

## 完了（2026-02-18）

- [x] Notebook (.ipynb) のサイズ不整合で読み込みが途中で切れる問題（Exportサイズ反映、テスト）

## 完了（2026-03-07）

- [x] Notebook を source 表示に変更（言語別拡張子、SOURCE import/export、衝突時 `.ipynb` フォールバック、rename/テスト更新）
- [x] Notebook rename 安全性修正（dirty/open rename の pre-flush、言語変更 rename 後の inode/cache refresh、rename テスト補強）
- [x] errno 正規化で VSCode 互換を改善（ENOTEMPTY/EEXIST/ENOENT/EACCES/EINVAL のマッピング、recursive delete テスト補強）
- [x] 非空ディレクトリ削除の errno 修正（wrapped/stringified `DIRECTORY_NOT_EMPTY` を `ENOTEMPTY` に正規化、VSCode `rmdir` 回帰テスト追加）
- [x] zero-config cache へ移行（legacy cache CLI 廃止、always-on metadata/dir cache、default disk cache、singleflight、Open の lazy read、cache テスト更新）
- [x] legacy cache option cleanup（systemd/maintainer docs/CLI テストから stale 参照を削除）

## 完了（2026-03-08）

- [x] 依存更新（`github.com/databricks/databricks-sdk-go` v0.118.0 / `github.com/hanwen/go-fuse/v2` v2.9.0、Go test と Docker FUSE test を確認）
- [x] ソース checkout の実行導線を Docker shell に統一（`scripts/run_wsfs_docker.sh` 追加、AGENTS/README から direct-run 導線を削除、macOS/Linux の案内を統一）
- [x] Open 時の freshness 強化（clean な通常ファイルを read-only open でリモート再検証、変更検知時は clean buffer / metadata cache / disk cache を無効化し `DIRECT_IO` を返す）
- [x] ディスクキャッシュ hardening（checksum 追跡、missing/corrupt cache の invalidate + remote retry、mutation 前の整合チェック）
- [x] dirty な通常ファイル rename 安全化（backend rename 前の pre-flush、失敗時 abort、rename 後 refresh で clean/cache state をクリア）
- [x] stat の owner 表示を mount owner UID/GID に固定（`NodeConfig.OwnerGid` 追加、caller 依存の `stat` を廃止）
- [x] timestamp-only `Setattr` を `ENOTSUP` に統一（`touch existing-file` / `os.utime` 系、size change 併用時は timestamp 指定を無視）
- [x] docs/tests 整合（`docs/behavior.md` 新設、README 更新、security/cache/fuse shell test の期待値修正）
- [x] `chmod` を互換 no-op success に変更して Git 互換改善（`git init` の lockfile 権限調整を許容、Setattr/FUSE テスト更新）
- [x] Docker テスト導線と VSCode テスト配置を整理（`scripts/test_docker.sh` / `scripts/test_vscode_docker.sh` / `scripts/tests/run.sh` / `scripts/tests/vscode/` へ統一、README/AGENTS/CLAUDE 更新）
- [x] `touch new-file` 互換を回復（新規空ファイルの post-create timestamp sync を no-op success に限定、FUSE `os.utime` テストの quoting も修正）
- [x] `AGENTS.md` を簡潔化（`CLAUDE.md` から参照する前提で必須ワークフローと主要コマンドに整理）
- [x] `AGENTS.md` をさらに簡潔化（試験は必要時のみ、まず Go unit test を優先する方針に更新）
- [x] `AGENTS.md` をさらに整理（コマンド説明追加、VSCode test/TS 型チェック方針を明記、`Preserve` を短い Notes に集約）
- [x] `AGENTS.md` に `gofmt` 方針を追加（Go 変更時は touched files を先に整形する運用を明記）
- [x] リリース成果物を Linux-only に整理（unsupported な `darwin` archive を削除、README の `.deb` install/update を `apt install ./...` に更新）
- [x] release artifact に third-party licenses を同梱（`go-licenses save` を release 前 hook に追加し、`tar.gz` / `.deb` に同梱）

## 完了（2026-03-09）

- [x] 検索/索引 workload を TTL ベース再検証へ変更（read-only open の forced revalidate 廃止、clean child Lookup 再利用、VSCode/rg 診断と docs 更新）
- [x] `--allow-other` stale metadata 回帰を修正（local write 後は `StatFresh` で即時整合、post-write cache invalidate、Create fallback、security/stat 回帰テスト追加）

## 完了（2026-03-10）

- [x] Databricks hidden `projects` API 調査メモを追加（`docs/projects-api-investigation.md`）
- [x] Databricks hidden `projects` API を追加調査（`create` / `fetch-and-checkout` / `update` / `clone` / `delete` の実挙動確認）
- [x] Databricks hidden `projects` API を追加深掘り（`status` / `diff` / `add` / `commit` 系 REST の不在と private GraphQL 境界を確認）
- [x] Databricks hidden `/api/2.0/git` family を追加調査（`git` / `repo-git` / `project-git` / `git-ops` 系は `ENDPOINT_NOT_FOUND` を確認）

## 完了（2026-03-11）

- [x] VSCode E2E ワークスペース cleanup を追加（`scripts/tests/vscode/run_in_container.sh` で成功/失敗時ともに `vscode_e2e_*` を削除）
- [x] Git metadata path を高速化（notebook size export を metadata path から除去、dir-first Lookup、zero-config TTL defaults、`git_diagnostic.sh`、docs/test 更新）
- [x] Git smoke を標準 FUSE テストへ統合（`fuse_test.sh` に `git status/add/commit/rev-parse/log` を追加し、`git_diagnostic.sh` は手動診断に整理）
- [x] Git 安定化 1-4（notebook exact size の永続化、flush 後 exact size 保持、new regular file first-write fast path、regular flush の local metadata 化、Git 回帰テスト追加）
- [x] cold notebook の `stat/read` size 不整合を修正（metadata path で exact size を materialize、`Getattr`/`Lookup`/read-only `Open` の回帰テスト追加、docs 更新）

---

## 未対応（オプション）

### パフォーマンス

- [ ] 書き込みの一時ファイル経由化（巨大ファイル時のメモリ削減）
- [ ] fusePath/remotePath の型安全ラッパー

### 観測性・運用性

- [ ] メトリクス出力（read/write/stat/err）
- [ ] Prometheus 形式でのエクスポート
- [ ] レート制御（Databricks API制限対応）

### 配布

- [ ] GitHub Actions でバイナリ配布（goreleaser）
- [x] .deb パッケージ対応（brewなし）

### ドキュメント

- [ ] README.md の簡素化

---

## 未解決の課題（要検証）

- [ ] 同一ファイルへの並列書き込みの競合（last-write-wins）
- [ ] メタキャッシュのstale read（Write後、TTL内のStatが古い可能性）
- [ ] Release失敗時のdirtyバッファ保持（メモリ解放遅延）

---

## 既知の制限事項

- Statfs は固定値
- 既存ファイルへの atime-only / mtime-only 更新は ENOTSUP（新規空ファイルの初回 post-create timestamp sync のみ互換 no-op success）、`chmod` は互換 no-op success、`chown` は ENOTSUP
- `new-files` signed URL upload は 403 の場合あり（フォールバックで対応）
- 推奨: 単一ユーザー開発用途 / CI / ローカル編集
- 非推奨: チーム共有サーバー / 本番運用 / 機密データ環境
