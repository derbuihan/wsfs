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
- [ ] `object-info` の signed URL を使った read path を追加。
- [ ] 失敗時は `workspace.Export` にフォールバック。

### P2-2: 書き込み強化
- [ ] `new-files` / `write-files` のトライパスを追加。
- [ ] 失敗時は `import-file` にフォールバック。

**完了条件**
- 大きいファイルの読み書きテストが通る（新規テスト追加）。

---

## Phase 3: キャッシュ導入（性能・安定性向上）

### P3-1: 最小のローカルキャッシュ
- [ ] ローカル staging file 方式（読み取りキャッシュのみ）
- [ ] 破棄ルール（TTL or LRU）

### P3-2: 書き込みバッファ方式の整理
- [ ] `Flush/Release` で確実に書き戻し
- [ ] 破損時の回復パス（再取得/再アップロード）

**完了条件**
- キャッシュON/OFFの切替可能。
- キャッシュ破損時でも安定動作。

---

## Phase 4: 本家 wsfs の安定性に近づける

### P4-1: スケール耐性
- [ ] 並行アクセス、rename競合、トランケート連発の耐性をテスト強化。
- [ ] ロック/排他制御の整理。

### P4-2: エラー処理強化
- [ ] 失敗時ログ + フォールバック
- [ ] リトライ/バックオフ戦略（必要箇所のみ）

**完了条件**
- 長時間運用で落ちない（簡易ストレステスト追加）。

---

## Phase 5: ドキュメント + リリース品質

- [ ] README に本番運用の注意点を追加。
- [ ] Docker/CI 用の安定テストシナリオを整備。
- [ ] 重大リグレッションを検出するテストを固定化。

---

## 追加するテスト候補
- [ ] 10MB〜100MBのファイル read/write
- [ ] 連続 truncate
- [ ] 連続 rename/mv
- [ ] 複数同時 write
- [ ] キャッシュ ON/OFF 両方で同じ結果

---

## 最初の実装順序（推奨）
1. Phase R（最小リファクタリング + テスト足場）
2. Phase 1（互換性強化）
3. Phase 2（データパス強化）
4. Phase 3（キャッシュ導入）
5. Phase 4（安定化・ストレス耐性）
