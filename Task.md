# Future Tasks (wsfs)

このファイルは今後のオプション機能と、未解決の課題だけを短くまとめる。

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
- [ ] Open時のリモート更新チェックで古いデータが返る可能性
- [ ] メタキャッシュのstale read（Write後、TTL内のStatが古い可能性）
- [ ] ディスクキャッシュのTOCTOU
- [ ] Release失敗時のdirtyバッファ保持（メモリ解放遅延）

---

## 既知の制限事項

- Statfs は固定値
- atime-only 更新は ENOTSUP、chmod/chown も ENOTSUP
- `new-files` signed URL upload は 403 の場合あり（フォールバックで対応）
- 推奨: 単一ユーザー開発用途 / CI / ローカル編集
- 非推奨: チーム共有サーバー / 本番運用 / 機密データ環境
