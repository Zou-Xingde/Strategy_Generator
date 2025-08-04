import pandas as pd
import os

files = {
    "XAUUSD": r"D:\project\策略產生器\data\Strategy_GeneratorXAUUSD_dukascopy_TICK_UTC-TICK-No Session.csv",
    "US30": r"D:\project\策略產生器\data\Strategy_GeneratorUSA30IDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv",
    "US100": r"D:\project\策略產生器\data\Strategy_GeneratorUSATECHIDXUSD_dukascopy_TICK_UTC-TICK-No Session.csv"
}

for name, filepath in files.items():
    print(f"\n=== {name} ===")
    if not os.path.exists(filepath):
        print(f"❌ 檔案不存在: {filepath}")
        continue
    
    try:
        df = pd.read_csv(filepath, nrows=5)
        print(f"✅ 檔案存在，共 {len(df)} 列")
        print(f"📊 欄位: {list(df.columns)}")
        print(f"📋 前3筆資料:")
        print(df.head(3).to_string(index=False))
        print(f"📐 資料型別:")
        print(df.dtypes.to_string())
    except Exception as e:
        print(f"❌ 讀取錯誤: {e}") 