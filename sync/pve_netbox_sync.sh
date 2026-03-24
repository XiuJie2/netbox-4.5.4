#!/bin/bash

# 設定環境變數
export NB_API_URL="https://localhost"
export NB_API_TOKEN="9698l7dj8fw0fgob9e92df8d8v8f8gedc00vnt00f0rwwsfg35r"

export PVE_API_HOST="192.168.2.180"
export PVE_API_USER="root@pam"
export PVE_API_TOKEN="netbox"
export PVE_API_SECRET="bef817b0-bde4-4370-acbd-1i34n1mdmwk"
export PVE_API_VERIFY_SSL="false"
export NB_CLUSTER_ID="2"

export TELEGRAM_BOT_TOKEN="7725700765:AAFBEEx_9Dl9tl-fdkldklKDJHLkndwjsKLdksOo"
export TELEGRAM_CHAT_ID="-183952023501"

# 設定路徑
SCRIPT_DIR="/opt/py-api"
LOG_DIR="/home/birc/logs/netbox-pve-sync"
LOG_FILE="$LOG_DIR/sync_$(date +'%Y%m%d_%H%M%S').log"
ERROR_LOG="$LOG_DIR/error.log"

# 創建日誌目錄
mkdir -p $LOG_DIR

# 開始執行
echo "========== 開始同步 $(date) ==========" >> $LOG_FILE

cd $SCRIPT_DIR

# 執行同步腳本
/opt/netbox/py-api/venv/bin/python /opt/netbox/py-api/sync.py >> $LOG_FILE 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "同步完成，退出碼: $EXIT_CODE" >> $LOG_FILE
else
    echo "同步失敗，退出碼: $EXIT_CODE" >> $LOG_FILE
    echo "$(date): 同步失敗，退出碼: $EXIT_CODE" >> $ERROR_LOG
    # 可以添加郵件通知或其他告警機制
fi

echo "========== 同步結束 $(date) ==========" >> $LOG_FILE

# 刪除超過30天的日誌
find $LOG_DIR -name "sync_*.log" -mtime +7 -delete

exit $EXIT_CODE
