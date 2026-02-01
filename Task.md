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
- [ ] 並行アクセス、rename競合、トランケート連発の耐性をテスト強化。
- [ ] ロック/排他制御の整理。

### P4-2: エラー処理強化
- [ ] 失敗時ログ + フォールバック
- [ ] リトライ/バックオフ戦略（必要箇所のみ）
- [ ] HTTPタイムアウトの見直し（現在5分 → 適切な値に調整）
- [ ] エラーメッセージから機密情報を除去

### P4-3: セキュリティ強化
- [ ] `Access()` の実装改善（UID/GIDベースのアクセス制御）
- [x] `AllowOther` をデフォルト無効化（`--allow-other` フラグで有効化可能に）
- [x] キャッシュファイルのパーミッションを `0600` に修正
- [x] キャッシュディレクトリのパーミッションを `0700` に修正
- [x] パストラバーサル対策（`path.Clean()` + 親ディレクトリチェック）

### P4-4: コード品質改善
- [x] タイポ修正（"Faild" → "Failed" in main.go）
- [ ] エラーハンドリングの一貫性向上
- [ ] マジックナンバーの定数化（Statfs の値など）

**完了条件**
- 長時間運用で落ちない（簡易ストレステスト追加）。

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
- [ ] graceful shutdown（シグナルハンドリング）の実装
- [ ] ログレベル設定の追加（`--log-level` フラグ）
- [ ] メトリクス出力（オプション、Prometheus形式など）の検討

### P5-3: テスト・CI整備
- [ ] Docker/CI 用の安定テストシナリオを整備
- [ ] 重大リグレッションを検出するテストを固定化

---

## 追加するテスト候補
- [x] 10MB〜100MBのファイル read/write (`scripts/large_file_test.sh` で10MB実装済み、50MB/100MBはオプション)
- [x] キャッシュ ON/OFF 両方で同じ結果 (`scripts/docker_cache_test.sh` Test 1 & 2 で検証済み)
- [x] Databricks公式実装との整合性検証 (`scripts/databricks_cli_verification_test.sh` で完全検証済み)
- [ ] 連続 truncate
- [ ] 連続 rename/mv
- [ ] 複数同時 write

---

## 最初の実装順序（推奨）
1. Phase R（最小リファクタリング + テスト足場）
2. Phase 1（互換性強化）
3. Phase 2（データパス強化）
4. Phase 3（キャッシュ導入）
5. Phase 4（安定化・ストレス耐性・セキュリティ）
6. Phase 5（ドキュメント + リリース品質）

---

## 既知の制限事項

### セキュリティ関連（Phase 4で対応予定）
- `Access()` は現在楽観的許可（常に成功を返す）→ UID/GIDチェック未実装
- `AllowOther` がデフォルト有効 → マルチユーザー環境で他ユーザーがアクセス可能
- キャッシュファイルは `0644` → 他ユーザーから読める可能性あり
- パストラバーサル対策が未実装

### 機能制限
- Permissions は未対応（Access は常に許可）
- `Statfs` は合成された固定値を返す（実際のワークスペース容量を反映しない）
- atime-only 更新は `ENOTSUP`
- chmod/chown は `ENOTSUP`
- `new-files` signed URL upload は 403 を返す場合あり（フォールバックで対応）
- `write-files` の正確なリクエストフォーマットは不明（フォールバックで対応）

### 既知のバグ/改善点
- キャッシュ復旧時に `remotePath` を復元できない（ハッシュから逆算不可）
- HTTPタイムアウトが5分と長い
