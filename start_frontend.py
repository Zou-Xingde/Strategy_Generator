#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速啟動前端腳本
用於測試新的FastAPI前端界面
"""

import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import uvicorn
from config.settings import FRONTEND_HOST, FRONTEND_PORT

if __name__ == "__main__":
    print("🚀 啟動高性能FastAPI前端...")
    print(f"📊 訪問地址: http://{FRONTEND_HOST}:{FRONTEND_PORT}")
    print("✨ 使用 Ctrl+C 停止服務器")
    print("=" * 50)
    
    try:
        uvicorn.run(
            "src.frontend.app:app",
            host=FRONTEND_HOST,
            port=FRONTEND_PORT,
            reload=True,
            log_level="info"
        )
    except KeyboardInterrupt:
        print("\n👋 服務器已停止")
    except Exception as e:
        print(f"❌ 啟動失敗: {e}")
        print("請檢查:")
        print("1. 是否已安裝所有依賴項: pip install -r requirements.txt")
        print("2. 端口是否被佔用")
        print("3. 資料庫文件是否存在") 