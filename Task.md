# Task Plan for wsfs (stability-first)

目的: 既存コードを壊さずに段階的に本家 wsfs 相当の安定性/互換性へ近づける。変更は小さく、常にテストで担保する。

## 方針 (変えないこと)
- 小さなPRサイズ・段階的リリース。
- 既存の動作・テスト (`scripts/fuse_test.sh`) を毎段階で維持。
- 追加機能は必ず **フェイルセーフ** (ENOTSUP / 既存パスにフォールバック) で導入。
- 可能な限りシンプルな設計を優先し、必要な箇所のみリファクタリング。

---

## Phase R: 事前リファクタリング（安全に拡張するための土台）

### R1: 境界の明確化（小さく）
- [x] `WorkspaceFilesClient` の I/O を **インターフェース化** (`WorkspaceFilesAPI` など) し、スタブ/テスト注入を可能にする。
- [x] `WSNode` の「データ保持/flush/dirty」周りを **小さなユニット**（例: `FileBuffer`）に分離。
- [x] ログ出力は `debugf` に統一し、通常時のノイズを削減。

### R2: テストしやすさの確保
- [x] `client.go` の API 呼び出し部分に **薄いラッパ**を入れてモック可能にする。
- [x] `node.go` のバッファ挙動について **ユニットテストの足場**を作る。

**完了条件**
- 既存の `scripts/fuse_test.sh` が通る。
- 変更差分が小さく、挙動は変わらない。

---

## Phase 0: 仕様・挙動の確定（実装の安全性）

### P0-1: 仕様整理
- [x] README / docs の「現在の挙動」と「未対応」を明確化。
- [x] `Setattr` / `Open` / `Flush` の期待動作を明文化。

### P0-2: API 実挙動の検証
- [x] `workspace-files/new-files` / `write-files` / signed URL upload の検証手順を作成。
- [x] 失敗時のフォールバック経路を明確化（Export/import-file）。

**完了条件**
- `docs/` に検証結果が残っている。

---

## Phase 1: 互換性強化（go-fuse 未実装の優先対応）

### P1-1: 影響の大きいインターフェースの実装
- [x] `NodeRelease`（バッファ解放 + flush 保障）
- [x] `NodeAccess`（楽観的許可 or 明示 ENOTSUP）
- [x] `NodeStatfs`（安定した疑似値を返す）
- [x] `NodeOpendir`（最低限の整合）
- [x] `NodeOpendirHandle`（必要なら追加）
- [x] `NodeOnForget`（inodeキャッシュ整理）

### P1-2: メタデータ挙動の明確化
- [x] `Setattr` の atime/mtime 更新の一貫性
- [x] 失敗コードの整理（EISDIR/ENOTSUP など）

**完了条件**
- `scripts/fuse_test.sh` が通る。
- `docs/go-fuse-unimplemented.md` が更新される。

---

## Phase 2: データパス強化（大きいファイル安定化）

### P2-1: 読み込み強化
- [x] `object-info` の signed URL を使った read path を追加。
- [x] 失敗時は `workspace.Export` にフォールバック。
- [x] 大きいファイルテストスクリプト `scripts/large_file_test.sh` を追加。

### P2-2: 書き込み強化
- [x] `new-files` / `write-files` のトライパスを追加。
- [x] 失敗時は `import-file` にフォールバック。

**完了条件**
- P2-1: ✅ 完了。signed URL読み込みが実装され、フォールバックが動作する。
- P2-2: ✅ 完了。new-files/write-filesが実装され、フォールバックが動作する。10MB ファイルの書き込みテストが通る。

---

## Phase 3: キャッシュ導入（性能・安定性向上）

### P3-1: 最小のローカルキャッシュ
- [x] ローカル staging file 方式（読み取りキャッシュのみ）
- [x] 破棄ルール（TTL + LRU）
- [x] キャッシュON/OFFフラグ追加 (`--cache`, `--cache-dir`, `--cache-size`, `--cache-ttl`)
- [x] `ensureDataLocked()` でのキャッシュ統合（cache hit時はローカルから読み込み、cache miss時はリモートから取得してキャッシュ）
- [x] `Flush/Release` でキャッシュ更新
- [x] `Unlink/Rename` でキャッシュ削除
- [x] 包括的なテスト追加（13テスト、100%カバレッジ）

### P3-2: 書き込みバッファ方式の整理
- [x] `Flush/Release` で確実に書き戻し（既存の実装で対応済み）
- [ ] 破損時の回復パス（チェックサム検証、再取得/再アップロード）

### P3-3: キャッシュテスト実装
- [x] 基本キャッシュ動作テスト (`scripts/cache_test.sh` - 9カテゴリ)
- [x] キャッシュ同期テスト (`scripts/cache_sync_test.sh` - 4カテゴリ)
- [x] Databricks CLI検証テスト (`scripts/databricks_cli_verification_test.sh` - 8検証シナリオ)
- [x] 統合キャッシュテスト (`scripts/docker_cache_test.sh` - 4構成)
- [x] Goユニットテスト (`internal/filecache/disk_cache_test.go` - 13テスト、100%カバレッジ)

**完了条件**
- ✅ P3-1: 完了。キャッシュON/OFFの切替可能。ディスクキャッシュが実装され、LRU + TTL エビクションが動作する。
- ⏳ P3-2: 部分完了。Flush/Releaseは実装済み。破損検証は未実装（Phase 4で対応可能）。
- ✅ P3-3: 完了。すべてのキャッシュテストが実装され、Databricks公式CLIとの整合性検証も完了。

---

## Phase 4: 本家 wsfs の安定性に近づける

### P4-1: スケール耐性
- [ ] 並行アクセス、rename競合、トランケート連発の耐性をテスト強化
- [ ] ロック/排他制御の整理

### P4-2: エラー処理強化
- [x] 失敗時ログ + フォールバック（既存のフォールバック機構で対応済み）
- [x] リトライ/バックオフ戦略（429/5xx対応、指数バックオフ）
  - `internal/retry/` パッケージ追加
  - signed URL操作（read/write）で自動リトライ
  - 最大5回、指数バックオフ（1s→2s→4s→8s→16s）、Retry-Afterヘッダー対応
- [x] HTTPタイムアウトの見直し（5分 → 2分に調整）
- [x] エラーメッセージから機密情報を除去（sanitizeError/truncateBody関数を追加）
- [ ] errnoマッピングの整理（EACCES/ENOENT/EINVAL/EIO の使い分けポリシー策定）
- [ ] エラー握りつぶしの修正（flushLocked等で失敗時もログ出力）

### P4-3: セキュリティ強化
- [ ] `Access()` の実装改善（UID/GIDベースのアクセス制御）
- [x] `AllowOther` をデフォルト無効化（`--allow-other` フラグで有効化可能に）
- [x] キャッシュファイルのパーミッションを `0600` に修正
- [x] キャッシュディレクトリのパーミッションを `0700` に修正
- [x] パストラバーサル対策（`path.Clean()` + 親ディレクトリチェック）

### P4-4: コード品質改善
- [x] タイポ修正（"Faild" → "Failed" in main.go）
- [x] タイポ修正（"writting" → "writing" in node.go）
- [x] エラーハンドリングの一貫性向上（エラーログにパス情報を追加）
- [x] マジックナンバーの定数化（権限、タイムアウト、ブロックサイズ、Nlink、inode等）
- [x] 型アサーションの安全化（`ok` チェック追加でパニック防止）
- [x] 設定値の検証（負の値チェック、範囲チェック）

### P4-5: キャッシュ信頼性
- [x] キャッシュ永続化の改善（起動時に復元不可ファイルを削除）
- [x] metacache の最大サイズ制限追加（10,000 エントリ上限）

**完了条件**
- ✅ キャッシュ再起動後も容量計算が正確（孤立ファイルを削除することで一貫性を保証）
- ✅ metacache のメモリ使用量が予測可能（10,000 エントリ上限）

---

## Phase 5: ドキュメント + リリース品質

### P5-1: ドキュメント整備
- [ ] README に本番運用の注意点を追加
  - [ ] シングルユーザー環境での使用推奨を明記
  - [ ] AllowOther の危険性を記載
  - [ ] キャッシュディレクトリの権限について記載
- [ ] 推奨される使用シナリオを記載（ローカル開発、CI/CD、Databricksノートブック編集）
- [ ] 非推奨の使用シナリオを記載（共有サーバー、機密データ環境）

### P5-2: 運用機能
- [x] Graceful Shutdown の実装
  - [x] signal.NotifyContext でシグナルハンドリング
  - [x] dirty buffer の flush ポリシー策定（DirtyNodeRegistry で追跡）
  - [x] server.Unmount() の適切な呼び出し
- [x] ログレベル設定の追加（`--log-level` フラグ）
  - DEBUG/INFO/WARN/ERROR レベル対応
  - `--debug` フラグとの後方互換性維持
  - `internal/logging/logging_test.go` にユニットテスト追加
- [ ] 操作ごとの context.WithTimeout 設定

### P5-3: テスト・CI整備
- [ ] Docker/CI 用の安定テストシナリオを整備
- [ ] 重大リグレッションを検出するテストを固定化
- [x] `CopyToCache`, `sanitizeURL`, `sanitizeError` のユニットテスト追加
  - `internal/databricks/client_test.go`: TestSanitizeURL, TestSanitizeError, TestTruncateBody
  - `internal/filecache/disk_cache_test.go`: TestDiskCacheCopyToCache

---

## Phase 6: パフォーマンス・スケーラビリティ

> **優先度**: Phase 4/5 完了後に実施

### P6-1: メモリ効率改善
- [ ] `ReadAll()` のストリーミング対応
  - [ ] ディスクキャッシュから直接読み込み（全メモリ保持を回避）
  - [ ] mmap または逐次読み込みの検討
- [ ] 書き込みの一時ファイル経由化
  - [ ] メモリ保持を最小化
  - [ ] Flush/Release で一括アップロード

### P6-2: パス管理の改善
- [ ] fusePath/remotePath の構造体分離
- [ ] `.ipynb` 変換ロジックの入口/出口統一

### P6-3: 観測性・運用性（オプション）
- [ ] メトリクス出力（read/write/stat/err カウンタ）
- [ ] レート制御（オプション）
- [ ] Prometheus 形式でのエクスポート検討

**完了条件**
- 100MB+ ファイルでメモリ使用量が一定以下
- fusePath/remotePath の混乱がなくなる

---

## 追加するテスト候補
- [x] 10MB〜100MBのファイル read/write (`scripts/large_file_test.sh` で10MB実装済み、50MB/100MBはオプション)
- [x] キャッシュ ON/OFF 両方で同じ結果 (`scripts/docker_cache_test.sh` Test 1 & 2 で検証済み)
- [x] Databricks公式実装との整合性検証 (`scripts/databricks_cli_verification_test.sh` で完全検証済み)
- [ ] 連続 truncate
- [ ] 連続 rename/mv
- [ ] 複数同時 write
- [x] `CopyToCache` のテスト (`internal/filecache/disk_cache_test.go`: TestDiskCacheCopyToCache)
- [x] `sanitizeURL`, `sanitizeError` のテスト (`internal/databricks/client_test.go`: TestSanitizeURL, TestSanitizeError)
- [ ] 巨大ファイル（100MB+）のメモリ使用量テスト

---

## 最初の実装順序（推奨）
1. Phase R（最小リファクタリング + テスト足場）
2. Phase 1（互換性強化）
3. Phase 2（データパス強化）
4. Phase 3（キャッシュ導入）
5. Phase 4（安定化・ストレス耐性・セキュリティ）
6. Phase 5（ドキュメント + リリース品質）
7. Phase 6（パフォーマンス・スケーラビリティ）

---

## 既知の制限事項

> **利用シナリオ別の推奨度**
> - ✅ 単一ユーザー開発用途：利用可能
> - ⚠️ チーム共有/サーバ常駐/本番運用：非推奨（アクセス制御なし、キャッシュ不整合、メモリ設計の問題）

### セキュリティ関連
- `Access()` は現在楽観的許可（常に成功を返す）→ UID/GIDチェック未実装
  - `--allow-other` を有効にした場合、全ユーザーがDatabricksトークンの権限でアクセス可能
  - 改善案：`--allow-other` 有効時にマウントユーザーのUID/GID以外を拒否するオプション

### 設計・アーキテクチャ
- `.ipynb` パス変換の一貫性問題
  - fusePath と remotePath が混在（`Rename()` で内部パス更新時にズレが発生する可能性）
  - 改善案：fusePath/remotePath を構造体で分離し、変換は入口/出口で統一
- `ReadAll()` による全ファイルメモリ保持
  - 巨大ファイルでOOM/スワップリスク、複数同時アクセスでRSS急増
  - キャッシュがあっても「まずメモリに載せる」形になっており効果が薄い
  - 改善案：ディスクキャッシュをmmap/streamで読む、書きは一時ファイル経由

### 機能制限
- Permissions は未対応（Access は常に許可）
- `Statfs` は合成された固定値を返す（実際のワークスペース容量を反映しない）
- atime-only 更新は `ENOTSUP`
- chmod/chown は `ENOTSUP`
- `new-files` signed URL upload は 403 を返す場合あり（フォールバックで対応）
- `write-files` の正確なリクエストフォーマットは不明（フォールバックで対応）

### 堅牢性・信頼性
- ~~Graceful Shutdown がない~~ → P5-2 で対応済み
  - ~~Ctrl+C で終了時、dirty buffer がフラッシュされない可能性~~
  - signal.NotifyContext + DirtyNodeRegistry + server.Unmount() で実装
- ~~metacache に最大サイズ制限がない（大量のファイルでメモリ消費増加の可能性）~~ → P4-5 で対応済み（10,000 エントリ上限）
- ~~型アサーションの安全性（一部でパニックの可能性あり）~~ → P4-4 で対応済み
- ~~キャッシュ再起動時の容量不整合~~ → P4-5 で対応済み（起動時に孤立ファイルを削除）
- エラーの握りつぶし・誤ったerrnoマッピング
  - `flushLocked()` で `Stat` 更新失敗しても成功扱い
  - `validateChildPath` エラーを `ENOENT` にしている（`EINVAL` が適切な場面も）
  - `ensureDataLocked()` は `ReadAll` 失敗を `EIO` へ丸める（原因切り分け不可）

### プロダクト運用
- ~~リトライ/バックオフ戦略がない~~ → P4-2 で対応済み（signed URL操作で429/5xxに対する指数バックオフリトライ）
- レート制御がない
- タイムアウトの一貫した設定がない（操作ごとの `context.WithTimeout`）
- 観測性の不足（メトリクス、read/write/stat/err カウンタ）

### コード品質
- ~~設定値の検証がない（負の値やエラーチェックが不十分）~~ → P4-4 で対応済み（キャッシュ設定の検証を追加）
- ~~ログレベルの制御が不十分（DEBUG/INFO/WARN/ERROR の区別なし）~~ → P5-2 で対応済み（`--log-level` フラグ追加）

### テスト関連
- ~~`CopyToCache`、`sanitizeURL`、`sanitizeError` のテストがない~~ → P5-3 で対応済み（既存の実装を確認）
- 並行アクセステストが不十分

### 既知のバグ/改善点
- ~~キャッシュ復旧時に `remotePath` を復元できない（ハッシュから逆算不可）~~ → P4-5 で対応済み（起動時に孤立ファイルを削除して一貫性を保証）
