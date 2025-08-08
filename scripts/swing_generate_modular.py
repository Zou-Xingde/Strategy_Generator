#!/usr/bin/env python3
"""
Modular swing generation script.
This script is launched by the FastAPI background task and should:
- read CLI args: --symbol, --timeframe, --algo, --params (JSON)
- produce progress logs to stdout, including lines containing percentage like '42%'
The FastAPI backend parses these lines to update progress and forward logs over WebSocket.
"""

import argparse
import json
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--symbol", required=True)
    p.add_argument("--timeframe", required=True)
    p.add_argument("--algo", default="zigzag")
    p.add_argument("--params", default="{}")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    try:
        params = json.loads(args.params) if isinstance(args.params, str) else (args.params or {})
    except Exception:
        params = {}

    # Example staged progress. Replace with real computation as needed.
    stages = [
        ("validate inputs", 5),
        ("load candlestick data", 20),
        ("run algorithm", 60),
        ("aggregate results", 80),
        ("write to database", 95),
        ("finalize", 100),
    ]

    print(f"[swing] start symbol={args.symbol} timeframe={args.timeframe} algo={args.algo} params={json.dumps(params, ensure_ascii=False)}", flush=True)
    for label, pct in stages:
        print(f"[swing] {label} ... {pct}%", flush=True)
        # Explicit progress marker for WS parser
        try:
            print(f"PROGRESS {float(pct)}", flush=True)
        except Exception:
            pass
        # Simulate work
        time.sleep(0.5)

    print("[swing] done 100%", flush=True)
    print("PROGRESS 100.0", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())


