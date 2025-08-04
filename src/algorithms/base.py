"""
演算法基礎類別

定義所有波段識別演算法的通用介面和抽象方法
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
import numpy as np
from datetime import datetime


class BaseAlgorithm(ABC):
    """波段識別演算法基礎類別"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.parameters: Dict[str, Any] = {}
    
    @abstractmethod
    def calculate(self, df: pd.DataFrame, **kwargs) -> pd.DataFrame:
        """
        執行演算法計算
        
        Args:
            df: 包含OHLCV資料的DataFrame
            **kwargs: 演算法特定參數
            
        Returns:
            包含波段識別結果的DataFrame
        """
        pass
    
    @abstractmethod
    def get_swing_points(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        獲取波段轉折點
        
        Args:
            df: 包含演算法計算結果的DataFrame
            
        Returns:
            轉折點列表，每個點包含時間戳、價格、類型等資訊
        """
        pass
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        驗證輸入資料格式
        
        Args:
            df: 要驗證的DataFrame
            
        Returns:
            是否有效
        """
        required_columns = ['open', 'high', 'low', 'close']
        
        if df.empty:
            return False
            
        if not all(col in df.columns for col in required_columns):
            return False
            
        # 檢查是否有空值
        if df[required_columns].isnull().values.any():
            return False
            
        # 檢查價格邏輯
        if not all(df['high'] >= df['low']) or not all(df['high'] >= df['open']) or not all(df['high'] >= df['close']):
            return False
            
        if not all(df['low'] <= df['open']) or not all(df['low'] <= df['close']):
            return False
            
        return True
    
    def get_algorithm_info(self) -> Dict[str, Any]:
        """
        獲取演算法資訊
        
        Returns:
            演算法詳細資訊
        """
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "type": self.__class__.__name__
        }
    
    def set_parameters(self, **kwargs):
        """
        設定演算法參數
        
        Args:
            **kwargs: 參數名稱和值
        """
        self.parameters.update(kwargs)
    
    def get_parameters(self) -> Dict[str, Any]:
        """
        獲取當前參數設定
        
        Returns:
            參數字典
        """
        return self.parameters.copy() 