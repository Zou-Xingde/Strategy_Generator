#!/usr/bin/env python3
# --- bootstrap sys.path for src imports ---
import sys as _sys
import pathlib as _pathlib
_ROOT = _pathlib.Path(__file__).resolve().parents[1]
_sys.path.insert(0, str(_ROOT))            # <repo_root>
_sys.path.insert(0, str(_ROOT / "src"))    # <repo_root>/src
# -----------------------------------------
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
import logging
from datetime import datetime
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
    # date-range overrides for single-run
    p.add_argument("--start", help="Start date YYYY-MM-DD (inclusive)")
    p.add_argument("--end", help="End date YYYY-MM-DD (inclusive)")
    p.add_argument("--last-days", type=int, help="If provided, derive start/end as [today-last_days, today]")
    # logging
    p.add_argument("--log-file", default=None, help="Path to log file; default logs/swing_generate_YYYYMMDD.log")
    p.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Logging level")
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


def iter_tasks(matrix: Dict[str, Any]) -> List[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]]:
    """Yield (symbol, timeframe, algo, params, window) from matrix.

    window supports keys: start, end, last_days.
    """
    tasks: List[Tuple[str, str, str, Dict[str, Any], Dict[str, Any]]] = []
    default_entry = matrix.get("default", {})
    default_window = dict(default_entry.get("window", {}))
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
                    window = dict(default_window)
                    window.update((item or {}).get("window", {}))
                    if algo:
                        tasks.append((symbol, timeframe, algo, dict(params), window))
            else:
                algo = (entry or {}).get("algo") or default_entry.get("algo")
                params = dict(default_entry.get("params", {}))
                params.update((entry or {}).get("params", {}))
                window = dict(default_window)
                window.update((entry or {}).get("window", {}))
                if algo:
                    tasks.append((symbol, timeframe, algo, params, window))
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


def setup_logging(args: argparse.Namespace) -> logging.Logger:
    logs_dir = ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    if args.log_file:
        log_path = Path(args.log_file)
        if not log_path.is_absolute():
            log_path = logs_dir / log_path
    else:
        log_path = logs_dir / f"swing_generate_{datetime.now().strftime('%Y%m%d')}.log"

    level = getattr(logging, (args.log_level or "INFO").upper(), logging.INFO)
    formatter = logging.Formatter(fmt="%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.setLevel(level)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    sh.setLevel(level)

    logger = logging.getLogger("swing_generate")
    logger.setLevel(level)
    # Avoid duplicate handlers if main() reruns in same interpreter
    if not logger.handlers:
        logger.addHandler(fh)
        logger.addHandler(sh)
    logger.debug(f"Logging initialized -> {log_path}")
    return logger


def main() -> int:
    args = parse_args()
    logger = setup_logging(args)
    matrix_path = Path(args.config)
    try:
        matrix = load_matrix(matrix_path)
    except Exception as e:
        print(f"[error] failed to load matrix: {e}", file=sys.stderr)
        logging.getLogger("swing_generate").exception("failed to load matrix")
        return 2

    overrides_params = parse_algo_params(args)

    # Build task list
    if args.symbol and args.timeframe and (args.algo or overrides_params or args.start or args.end or args.last_days):
        algo_spec = args.algo or (matrix.get("default", {}).get("algo"))
        params = dict(matrix.get("default", {}).get("params", {}))
        params.update(overrides_params)
        # window from CLI
        window: Dict[str, Any] = {}
        if args.start:
            window["start"] = args.start
        if args.end:
            window["end"] = args.end
        if args.last_days is not None:
            window["last_days"] = int(args.last_days)
        tasks = [(args.symbol, args.timeframe, algo_spec, params, window)]  # type: ignore
    else:
        tasks = iter_tasks(matrix)

    total = len(tasks)
    completed = 0
    failed = 0

    print(f"[swing] tasks: {total}")
    logger.info(f"config={matrix_path} tasks={total} dry_run={args.dry_run} batch_size={args.batch_size}")
    processor = SwingProcessor()

    for idx, (symbol, timeframe, algo_spec, params, window) in enumerate(tasks, start=1):
        # Progress mark (overall)
        percent = 0.0 if total == 0 else round((idx - 1) * 100.0 / total, 1)
        print(f"PROGRESS {percent}")
        print(f"[swing] run {idx}/{total} symbol={symbol} timeframe={timeframe} algo={algo_spec} params={json.dumps(params, ensure_ascii=False)}")
        logger.info(f"start idx={idx}/{total} symbol={symbol} timeframe={timeframe} algo={algo_spec} params={json.dumps(params, ensure_ascii=False)} window={json.dumps(window, ensure_ascii=False)}")

        algo_key = normalize_algo_name(str(algo_spec))
        try:
            if args.dry_run:
                # Simulate quick progress for CI/testing
                print(f"[dry-run] would process {symbol} {timeframe} {algo_key} with params={json.dumps(params, ensure_ascii=False)}")
            else:
                # Decide windowed vs count-based processing
                has_window = isinstance(window, dict) and (window.get('start') or window.get('end') or window.get('last_days'))
                if has_window:
                    # Derive start/end from window
                    start = window.get('start')
                    end = window.get('end')
                    if not (start or end) and window.get('last_days'):
                        # Pass last_days down as days in a smart helper? We already have by_date_range API, so compute dates here.
                        from datetime import datetime, timedelta
                        end_dt = datetime.now()
                        start_dt = end_dt - timedelta(days=int(window.get('last_days')))
                        start = start_dt.strftime('%Y-%m-%d')
                        end = end_dt.strftime('%Y-%m-%d')
                    logger.info(f"resolved window start={start} end={end}")
                    result = processor.process_symbol_timeframe_by_date_range(
                        symbol=symbol,
                        timeframe=timeframe,
                        algorithm_name=algo_key,
                        start_date=start,
                        end_date=end,
                        batch_size=args.batch_size,
                        **params,
                    )
                else:
                    # Fallback to count-based (limit inside DB layer if any)
                    result = processor.process_symbol_timeframe(
                        symbol=symbol,
                        timeframe=timeframe,
                        algorithm_name=algo_key,
                        batch_size=args.batch_size,
                        **params,
                    )
                print(f"[swing] finished {symbol} {timeframe} {algo_key}: {result.get('swing_points', 0)} swings")
                logger.info(f"finished symbol={symbol} timeframe={timeframe} algo={algo_key} swings={result.get('swing_points', 0)}")

            completed += 1
            # Mark per-task completion bump
            percent = round(idx * 100.0 / total, 1)
            print(f"PROGRESS {percent}")
        except Exception as e:
            failed += 1
            print(f"[error] task failed for {symbol} {timeframe} {algo_key}: {e}", file=sys.stderr)
            logger.exception(f"task failed symbol={symbol} timeframe={timeframe} algo={algo_key}")
            # continue to next

    # Finalize
    print("PROGRESS 100.0")
    print(f"[swing] summary: completed {completed}/failed {failed}")
    logger.info(f"summary completed={completed} failed={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())


