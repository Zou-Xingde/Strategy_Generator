"""
市場波段演算法模組

此模組包含各種用於識別市場波段的演算法：
- ZigZag: 經典的轉折點識別算法
- 未來將添加更多算法...
"""

from .zigzag import ZigZagAlgorithm
from .base import BaseAlgorithm

__all__ = ['ZigZagAlgorithm', 'BaseAlgorithm'] 