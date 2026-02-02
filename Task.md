# Future Tasks (wsfs)

このファイルは将来対応するオプション機能と、使用中に発見した課題を記録する場所です。

---

## 将来対応（オプション）

### パフォーマンス改善
- [ ] 書き込みの一時ファイル経由化（巨大ファイル書き込み時のメモリ使用量削減）
- [ ] fusePath/remotePath の型安全ラッパー（コンパイル時のバグ検出）

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

<!-- 使用中に発見した課題をここに追記 -->

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
