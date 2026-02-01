#!/bin/bash
set -e

# .envファイルから環境変数を読み込み
if [ -f /workspaces/wsfs/.env ]; then
  set -a
  source /workspaces/wsfs/.env
  set +a
fi

# wsfsをビルド
cd /workspaces/wsfs
go build -o /tmp/wsfs ./cmd/wsfs

# マウントポイント作成
sudo mkdir -p /mnt/databricks
sudo chown vscode:vscode /mnt/databricks

echo ""
echo "=================================="
echo "wsfs built successfully!"
echo "=================================="
echo ""
echo "To mount Databricks workspace, run:"
echo "  /tmp/wsfs /mnt/databricks"
echo ""
echo "To unmount:"
echo "  fusermount3 -u /mnt/databricks"
echo ""