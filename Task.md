# Future Tasks (wsfs)

このファイルは将来対応するオプション機能と、使用中に発見した課題を記録する場所です。

---

## 完了（2026-02-07）

- [x] FUSEノードの正しさ改善（openCount/dirtyFlags導入、truncate即時flush、Renameのサブツリー更新、テスト追加）

---

## 将来対応（オプション）

### パフォーマンス改善

- [ ] 書き込みの一時ファイル経由化（巨大ファイル書き込み時のメモリ使用量削減）
- [ ] fusePath/remotePath の型安全ラッパー（コンパイル時のバグ検出）
- [x] サイズベース API 選択戦略（5MB しきい値で実装済み）

### 観測性・運用性

- [ ] メトリクス出力（read/write/stat/err カウンタ）
- [ ] Prometheus 形式でのエクスポート
- [ ] レート制御（Databricks API制限対応）

### 配布

- [ ] GitHub Actions でバイナリ配布（goreleaser）
- [ ] brew / .deb パッケージ対応

### ドキュメント

- [x] docs/ 配下の整理（Workspace API仕様をまとめる）
- [ ] README.md の簡素化

---

## 使用中に発見した課題

### ファイル監視環境での書き込み競合問題

**発見日**: 2026-02-03
**再現環境**: Ubuntu Desktop + VSCode、stat連打がある環境

#### 症状

- VSCodeでファイルを編集・保存後、「Run Python」しても結果が出ない
- ファイル監視（stat連打）がある状態でファイルを上書きすると、読み込みが空になる
- 何度も保存すると「ファイルが外部で変更されました」と警告

#### Dockerでの再現テスト結果（2026-02-03）

```bash
# 高頻度stat連打（100Hz）中に echo > file を実行
echo 'NEW_LONGER_CONTENT_24BYTES' > /mnt/wsfs/test.txt

# 結果:
Content: ''
Size: 0 bytes
Expected: NEW_LONGER_CONTENT_24BYTES (24 bytes)
✗ FAIL: Race condition reproduced!
```

**確認された現象**:
1. **ファイル完全空化**: 高頻度stat連打中に`echo > file`を実行すると、ファイルが0バイトになる（最悪のケース）
2. **コンテンツ切り詰め**: ファイルサイズが途中で小さくなり、コンテンツが切り詰められる（24バイト→16バイト）
3. **サイズ変遷の一貫性なし**: statログで `Size: 24 → Size: 16 → Size: 0` のようにサイズが揺らぐ

#### 原因（確認済み）

1. **Setattr(truncate)時の即時flush**: `echo > file`でO_TRUNCが使われると、Setattr(size=0)で即座に空のデータがDatabricksに書き込まれる。その後のWriteが完了する前にstat連打によるLookupがDatabricksから空のデータを取得してしまう。

2. **mtimeの不整合**: flushLocked後にDatabricksからStatを取得してmtimeを更新するが、VSCodeが期待するmtimeと異なるため「外部変更」と誤認される。

#### 関連コード

- [node.go:456-459](internal/fuse/node.go#L456-L459) - Setattr内のflushLocked呼び出し
- [node.go:553-572](internal/fuse/node.go#L553-L572) - Open時のリモートチェック
- [node.go:526-533](internal/fuse/node.go#L526-L533) - LookupでのWSNode作成

#### 類似の潜在的問題

- [ ] **Open時のリモートチェックによる競合**（別プロセス読み込み時に古いデータが返る）
- [ ] **LookupでのWSNode再作成によるバッファ非共有**（同じファイルで異なるバッファ）
- [ ] **並列書き込み時のデータ競合**（複数プロセスのflushで最後が勝つ）
- [ ] **Getattrのロックなしアクセス**（Setattr→flushLocked中に古いサイズが返る）
- [ ] **メタキャッシュのstale read**（Write後、60秒TTL内のStatが古いメタデータを返す）
- [ ] **Release失敗時のバッファリーク**（ネットワークエラー時にdirtyバッファがクリアされない）
- [ ] **OnForgetによるバッファ消失**（カーネルメモリ圧力時にアクティブなRead/Write中のバッファがクリアされる）
- [ ] **ディスクキャッシュのTOCTOU**（Get()でRLock解除後のTTLチェック前にentryが更新される）
- [ ] **Rename後のパス不整合**（子ノードのfileInfo.Pathのみ更新され、並行操作は古いパスを参照）

#### ワークアラウンド

- VSCode設定: `"files.saveConflictResolution": "overwriteFileOnDisk"`
- Python実行はターミナルから直接行う
- ファイル監視を無効化（`files.watcherExclude`で除外）

#### 根本的な課題

これらの問題は**FUSEファイルシステムとリモートストレージの本質的な課題**:

1. **ローカルとリモートの状態同期**: Databricksへの書き込みには遅延があり、その間にLookupが走ると中間状態が見える
2. **POSIX互換性とパフォーマンスのトレードオフ**: 即時flushはPOSIXのtruncateセマンティクスに近いが、リモートストレージでは問題を引き起こす
3. **マルチプロセス環境での一貫性**: 単一プロセスでは問題ないが、ファイル監視などの並列アクセスで問題が顕在化

---

## 既知の制限事項

### 機能制限

- `Statfs` は合成された固定値を返す（実際のワークスペース容量を反映しない）
- atime-only 更新は `ENOTSUP`
- chmod/chown は `ENOTSUP`
- `new-files` signed URL upload は 403 を返す場合あり（フォールバックで対応）

### 利用シナリオ

- **推奨**: 単一ユーザー開発用途、CI/CD、ローカルでのノートブック編集
- **非推奨**: チーム共有サーバー、本番運用、機密データ環境

---

## 完了済み（参考）

以下のPhaseは全て完了:

- Phase R: リファクタリング（インターフェース化、テスト基盤）
- Phase 1: 互換性強化（go-fuse インターフェース実装）
- Phase 2: データパス強化（signed URL read/write、フォールバック）
- Phase 3: キャッシュ導入（LRU + TTL、チェックサム検証）
- Phase 4: 安定化（並行アクセス、リトライ、セキュリティ強化）
- Phase 5: ドキュメント・CI整備（Graceful Shutdown、ログレベル）
- Phase 6: パフォーマンス改善（オンデマンド読み込み、pathutil一元化）
