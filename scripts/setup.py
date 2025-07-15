#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
專案設置腳本
用於初始化專案環境和創建必要的目錄結構
"""

import os
import sys
from pathlib import Path

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import create_directories

def setup_environment():
    """設置專案環境"""
    print("正在設置專案環境...")
    
    # 創建目錄結構
    create_directories()
    
    # 檢查Python版本
    python_version = sys.version_info
    if python_version.major < 3 or (python_version.major == 3 and python_version.minor < 8):
        print("警告: 建議使用Python 3.8或更高版本")
    
    print("專案環境設置完成!")
    print("\n接下來的步驟:")
    print("1. 安裝依賴項: pip install -r requirements.txt")
    print("2. 運行資料處理: python src/data_processing/tick_processor.py")
    print("3. 啟動前端界面: python src/frontend/app.py")

def check_data_file():
    """檢查資料文件是否存在"""
    from config.settings import TICK_CSV_PATH
    
    if not os.path.exists(TICK_CSV_PATH):
        print(f"警告: 找不到資料文件 {TICK_CSV_PATH}")
        print("請確認資料文件路徑是否正確")
        return False
    else:
        print(f"資料文件檢查通過: {TICK_CSV_PATH}")
        return True

def main():
    """主函數"""
    print("=== 市場波段規律分析專案設置 ===")
    
    # 設置環境
    setup_environment()
    
    # 檢查資料文件
    check_data_file()
    
    print("\n設置完成!")

if __name__ == "__main__":
    main() 