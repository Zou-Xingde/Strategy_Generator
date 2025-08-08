#!/usr/bin/env python3
"""
Swing generation driver with matrix support.

Features
- Reads YAML matrix (configs/swing_matrix.yaml by default)
- Optional CLI overrides: --symbol/--timeframe/--algo/--algo-param/--params
- Calls core data-processing pipeline for each (symbol,timeframe,algo,params)
- Prints progress lines: "PROGRESS <float>"
- Continues on errors; prints to stderr; final summary "Completed X/Failed Y"
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # lazy fail later

# Make project root importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.data_processing.swing_processor import SwingProcessor  # noqa: E402


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=str(ROOT / "configs" / "swing_matrix.yaml"))
    p.add_argument("--symbol")
    p.add_argument("--timeframe")
    p.add_argument("--algo")
    # Allow multiple --algo-param key=value
    p.add_argument("--algo-param", action="append", default=[])
    # Back-compat JSON params
    p.add_argument("--params", default=None)
    p.add_argument("--batch-size", type=int, default=10000)
    p.add_argument("--overlap", type=int, default=0)
    p.add_argument("--dry-run", action="store_true", help="Simulate processing without DB writes")
    return p.parse_args()


def load_matrix(path: Path) -> Dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML not installed; cannot read matrix")
    if not path.exists():
        raise FileNotFoundError(f"matrix not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def parse_algo_params(args: argparse.Namespace) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if args.params:
        try:
            params.update(json.loads(args.params))
        except Exception:
            print("[warn] --params is not valid JSON; ignored", file=sys.stderr)
    for kv in (args.algo_param or []):
        if not isinstance(kv, str) or "=" not in kv:
            continue
        k, v = kv.split("=", 1)
        # try number cast
        try:
            if "." in v:
                params[k] = float(v)
            else:
                params[k] = int(v)
        except Exception:
            params[k] = v
    return params


def iter_tasks(matrix: Dict[str, Any]) -> List[Tuple[str, str, str, Dict[str, Any]]]:
    tasks: List[Tuple[str, str, str, Dict[str, Any]]] = []
    default_entry = matrix.get("default", {})
    for symbol, tf_map in matrix.items():
        if symbol == "default":
            continue
        if not isinstance(tf_map, dict):
            continue
        for timeframe, entry in tf_map.items():
            if isinstance(entry, list):
                for item in entry:
                    algo = (item or {}).get("algo")
                    params = (item or {}).get("params", {})
                    if algo:
                        tasks.append((symbol, timeframe, algo, dict(params)))
            else:
                algo = (entry or {}).get("algo") or default_entry.get("algo")
                params = dict(default_entry.get("params", {}))
                params.update((entry or {}).get("params", {}))
                if algo:
                    tasks.append((symbol, timeframe, algo, params))
    return tasks


def normalize_algo_name(spec: str) -> str:
    """Map module:Class to internal algorithm key expected by SwingProcessor.
    Currently supports ZigZag variants -> "zigzag"; others return lower-name fallback.
    """
    s = (spec or "").lower()
    if "zigzag" in s:
        return "zigzag"
    # Fallback to last token
    if ":" in s:
        s = s.split(":", 1)[1]
    if "." in s:
        s = s.split(".")[-1]
    return s or "zigzag"


def main() -> int:
    args = parse_args()
    matrix_path = Path(args.config)
    try:
        matrix = load_matrix(matrix_path)
    except Exception as e:
        print(f"[error] failed to load matrix: {e}", file=sys.stderr)
        return 2

    overrides_params = parse_algo_params(args)

    # Build task list
    if args.symbol and args.timeframe and (args.algo or overrides_params):
        algo_spec = args.algo or (matrix.get("default", {}).get("algo"))
        params = dict(matrix.get("default", {}).get("params", {}))
        params.update(overrides_params)
        tasks = [(args.symbol, args.timeframe, algo_spec, params)]  # type: ignore
    else:
        tasks = iter_tasks(matrix)

    total = len(tasks)
    completed = 0
    failed = 0

    print(f"[swing] tasks: {total}")
    processor = SwingProcessor()

    for idx, (symbol, timeframe, algo_spec, params) in enumerate(tasks, start=1):
        # Progress mark (overall)
        percent = 0.0 if total == 0 else round((idx - 1) * 100.0 / total, 1)
        print(f"PROGRESS {percent}")
        print(f"[swing] run {idx}/{total} symbol={symbol} timeframe={timeframe} algo={algo_spec} params={json.dumps(params, ensure_ascii=False)}")

        algo_key = normalize_algo_name(str(algo_spec))
        try:
            if args.dry_run:
                # Simulate quick progress for CI/testing
                print(f"[dry-run] would process {symbol} {timeframe} {algo_key} with params={json.dumps(params, ensure_ascii=False)}")
            else:
                # Note: overlap currently unused in processing; kept for CLI compatibility
                result = processor.process_symbol_timeframe(
                    symbol=symbol,
                    timeframe=timeframe,
                    algorithm_name=algo_key,
                    batch_size=args.batch_size,
                    **params,
                )
                print(f"[swing] finished {symbol} {timeframe} {algo_key}: {result.get('swing_points', 0)} swings")

            completed += 1
            # Mark per-task completion bump
            percent = round(idx * 100.0 / total, 1)
            print(f"PROGRESS {percent}")
        except Exception as e:
            failed += 1
            print(f"[error] task failed for {symbol} {timeframe} {algo_key}: {e}", file=sys.stderr)
            # continue to next

    # Finalize
    print("PROGRESS 100.0")
    print(f"[swing] summary: completed {completed}/failed {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())


