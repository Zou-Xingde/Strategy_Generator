#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd

# 讀取CSV文件
csv_path = r"D:\project\策略產生器\data\EX_D1USA30IDXUSD-D1-No Session.csv"

print("📖 檢查CSV文件內容...")
print(f"📁 文件路徑: {csv_path}")

try:
    # 讀取前幾行
    df = pd.read_csv(csv_path, nrows=20)
    
    print(f"✅ 成功讀取 {len(df)} 行資料")
    print(f"📊 欄位: {list(df.columns)}")
    print("\n📋 前10行資料:")
    print(df.head(10))
    
    print("\n📊 資料統計:")
    print(df.describe())
    
    print("\n🔍 檢查空值和零值:")
    for col in df.columns:
        if col != 'DateTime':
            zero_count = (df[col] == 0).sum()
            null_count = df[col].isnull().sum()
            print(f"{col}: 零值={zero_count}, 空值={null_count}")
    
    # 檢查是否有非零值
    if 'Bid' in df.columns and 'Ask' in df.columns:
        non_zero_bid = (df['Bid'] != 0).sum()
        non_zero_ask = (df['Ask'] != 0).sum()
        print(f"\n📈 非零Bid值: {non_zero_bid}")
        print(f"📈 非零Ask值: {non_zero_ask}")
        
        if non_zero_bid > 0:
            print("\n🔍 非零Bid值範例:")
            print(df[df['Bid'] != 0][['DateTime', 'Bid', 'Ask']].head())
        
except Exception as e:
    print(f"❌ 讀取失敗: {e}")
    import traceback
    traceback.print_exc() 