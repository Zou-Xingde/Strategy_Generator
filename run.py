#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
快速啟動腳本
用於快速啟動市場波段規律分析專案
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path

def run_setup():
    """運行設置腳本"""
    print("正在運行設置腳本...")
    subprocess.run([sys.executable, "scripts/setup.py"])

def run_data_processing():
    """運行資料處理"""
    print("正在啟動資料處理...")
    subprocess.run([sys.executable, "src/data_processing/tick_processor.py"])

def run_frontend():
    """運行前端界面"""
    print("正在啟動前端界面...")
    subprocess.run([sys.executable, "src/frontend/app.py"])

def run_tests():
    """運行測試"""
    print("正在運行測試...")
    subprocess.run([sys.executable, "-m", "pytest", "tests/", "-v"])

def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="市場波段規律分析專案啟動器")
    parser.add_argument("--setup", action="store_true", help="運行設置腳本")
    parser.add_argument("--process", action="store_true", help="運行資料處理")
    parser.add_argument("--frontend", action="store_true", help="啟動前端界面")
    parser.add_argument("--test", action="store_true", help="運行測試")
    parser.add_argument("--all", action="store_true", help="運行完整流程")
    
    args = parser.parse_args()
    
    print("=== 市場波段規律分析專案 ===")
    
    if args.setup or args.all:
        run_setup()
    
    if args.process or args.all:
        run_data_processing()
    
    if args.frontend or args.all:
        run_frontend()
    
    if args.test:
        run_tests()
    
    if not any(vars(args).values()):
        print("使用方法:")
        print("  python run.py --setup      # 運行設置")
        print("  python run.py --process    # 處理資料")
        print("  python run.py --frontend   # 啟動前端")
        print("  python run.py --test       # 運行測試")
        print("  python run.py --all        # 運行完整流程")

if __name__ == "__main__":
    main() 