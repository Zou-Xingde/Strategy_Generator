import unittest
import pandas as pd
import sys
import os

# 添加專案根目錄到路徑
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processing.tick_processor import TickProcessor
from src.database.connection import DuckDBConnection

class TestDataProcessing(unittest.TestCase):
    """資料處理模組測試"""
    
    def setUp(self):
        """測試前準備"""
        self.test_data = pd.DataFrame({
            'DateTime': pd.date_range('2023-01-01', periods=100, freq='1min'),
            'Bid': [100.0 + i * 0.01 for i in range(100)],
            'Ask': [100.01 + i * 0.01 for i in range(100)],
            'Volume': [100 + i for i in range(100)]
        })
    
    def test_tick_processor_init(self):
        """測試TickProcessor初始化"""
        processor = TickProcessor()
        self.assertIsNotNone(processor)
        self.assertIsInstance(processor.symbol, str)
    
    def test_standardize_columns(self):
        """測試列名標準化"""
        processor = TickProcessor()
        standardized = processor._standardize_columns(self.test_data)
        
        expected_columns = ['timestamp', 'bid', 'ask', 'volume']
        self.assertEqual(list(standardized.columns), expected_columns)
    
    def test_clean_data(self):
        """測試資料清洗"""
        processor = TickProcessor()
        
        # 準備測試資料
        test_data = self.test_data.copy()
        test_data.columns = ['timestamp', 'bid', 'ask', 'volume']
        
        # 添加異常值
        test_data.loc[50, 'bid'] = 0  # 異常值
        test_data.loc[51, 'ask'] = -1  # 異常值
        
        cleaned = processor._clean_data(test_data)
        
        # 檢查異常值已被移除
        self.assertTrue(all(cleaned['bid'] > 0))
        self.assertTrue(all(cleaned['ask'] > 0))
        self.assertTrue(all(cleaned['ask'] >= cleaned['bid']))
    
    def test_create_candlestick_data(self):
        """測試蠟燭圖資料生成"""
        processor = TickProcessor()
        
        # 準備測試資料
        test_data = self.test_data.copy()
        test_data.columns = ['timestamp', 'bid', 'ask', 'volume']
        
        # 生成蠟燭圖資料
        candlestick = processor.create_candlestick_data(test_data, 'M5')
        
        # 檢查結果
        self.assertIsInstance(candlestick, pd.DataFrame)
        if not candlestick.empty:
            expected_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            self.assertEqual(list(candlestick.columns), expected_columns)

if __name__ == '__main__':
    unittest.main() 