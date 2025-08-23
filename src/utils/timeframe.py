from __future__ import annotations
from typing import Optional
import re
from config.settings import TIMEFRAMES

# 常見別名（小寫）→ 設定鍵
_TF_ALIAS = {
    "1m": "M1", "5m": "M5", "15m": "M15", "30m": "M30",
    "1h": "H1", "4h": "H4", "1d": "D1",
}


def normalize_timeframe(tf: Optional[str]) -> str:
    """
    接受 1h/4h/1d 與 H1/H4/D1（大小寫皆可），輸出設定檔使用的鍵。
    不符合者盡量轉為大寫回傳（讓上游統一判斷）。
    """
    s = (tf or "").strip()
    if not s:
        return s
    low = s.lower()
    if low in _TF_ALIAS:
        return _TF_ALIAS[low]
    up = s.upper()
    if up in TIMEFRAMES:
        return up
    # 支援 "15m"/"1h"/"1d"
    m = re.fullmatch(r"(\d+)([mhd])", low)
    if m:
        return f"{m.group(1)}{m.group(2).upper()}"
    # 也容忍 "m15"/"h1"/"d1"
    m2 = re.fullmatch(r"([mhd])(\d+)", low)
    if m2:
        return f"{m2.group(1).upper()}{m2.group(2)}"
    return up




