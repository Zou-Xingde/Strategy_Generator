import asyncio
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Set

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException
import subprocess
import threading


router = APIRouter()


# Centralized progress store and local WS subscribers (legacy broadcast only)
try:
    from src.backend.progress_store import store as progress_store  # type: ignore
except Exception:  # fallback stub to avoid import-time errors
    progress_store = None  # type: ignore
subscribers: Dict[str, Set[WebSocket]] = {}


def _get_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


async def _broadcast(task_id: str, message: Dict[str, Any]) -> None:
    if task_id not in subscribers:
        return
    dead: List[WebSocket] = []
    for ws in list(subscribers[task_id]):
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        try:
            subscribers[task_id].discard(ws)
        except Exception:
            pass


def _parse_progress(line: str) -> float:
    # Extract the first occurrence of an integer percentage like '42%'
    try:
        idx = line.find('%')
        if idx > 0:
            # find number before %
            j = idx - 1
            digits = []
            while j >= 0 and line[j].isdigit():
                digits.append(line[j])
                j -= 1
            if digits:
                digits.reverse()
                value = int(''.join(digits))
                return float(max(0, min(100, value)))
    except Exception:
        pass
    return -1.0


import logging

logger = logging.getLogger(__name__)


async def _run_task_and_stream(
    task_id: str,
    symbol: str = "",
    timeframe: str = "",
    algo: str = "",
    params: Dict[str, Any] = None,
    config_path: str = "",
    batch_size: int = 0,
    overlap: int = 0,
) -> None:
    root = _get_project_root()

    # Ensure structures exist in centralized store
    try:
        if progress_store is not None:
            progress_store.set(task_id, {"stage": "started", "percent": 0, "logs": []})
    except Exception:
        pass
    subscribers.setdefault(task_id, set())

    try:
        # Setup environment for module execution and PYTHONPATH
        import os
        env = os.environ.copy()
        extra = os.pathsep.join([str(root), str(root / "src")])
        env["PYTHONPATH"] = extra + os.pathsep + env.get("PYTHONPATH", "")

        # Build module CLI
        args = [sys.executable, "-m", "scripts.swing_generate"]
        if config_path:
            args += ["--config", config_path]
        if symbol:
            args += ["--symbol", symbol]
        if timeframe:
            args += ["--timeframe", timeframe]
        if algo:
            args += ["--algo", algo]
        if params:
            args += ["--params", json.dumps(params, ensure_ascii=False)]
        if batch_size:
            args += ["--batch-size", str(batch_size)]
        if overlap:
            args += ["--overlap", str(overlap)]
        try:
            if params and isinstance(params, dict) and params.get("dry_run"):
                args += ["--dry-run"]
        except Exception:
            pass

        logger.info("CLI: %s", args)
        # Announce start to clients and store
        try:
            if progress_store is not None:
                progress_store.append_log(task_id, f"[server] start task {task_id}")
        except Exception:
            pass
        await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": f"[server] start task {task_id}"})

        try:
            # Preferred async subprocess (may fail on some Windows loop policies)
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(root),
                env=env,
            )

            assert proc.stdout is not None

            async for raw in proc.stdout:
                try:
                    line = raw.decode(errors="ignore").rstrip('\r\n')
                except Exception:
                    line = ""

                logger.debug("stdout: %s", line.strip())

                # Append log to centralized store
                try:
                    if progress_store is not None:
                        progress_store.append_log(task_id, line)
                except Exception:
                    pass
                await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": line})

                # Prefer explicit PROGRESS marker
                if line.startswith("PROGRESS "):
                    try:
                        pval = float(line.split(" ", 1)[1])
                        try:
                            if progress_store is not None:
                                progress_store.set(task_id, {"percent": max(0.0, min(100.0, pval))})
                        except Exception:
                            pass
                        await _broadcast(
                            task_id,
                            {"type": "progress", "task_id": task_id, "percent": pval},
                        )
                    except Exception:
                        pass
                else:
                    p = _parse_progress(line)
                    if p >= 0:
                        try:
                            if progress_store is not None:
                                progress_store.set(task_id, {"percent": p})
                        except Exception:
                            pass
                        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": p})

            # Wait return code
            await proc.wait()
        except NotImplementedError:
            # Windows fallback: use blocking subprocess in a thread and stream via queue
            loop = asyncio.get_running_loop()
            q: asyncio.Queue[str | None] = asyncio.Queue()

            def reader() -> None:
                p = subprocess.Popen(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    cwd=str(root),
                    env=env,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                )
                assert p.stdout is not None
                for line in p.stdout:
                    asyncio.run_coroutine_threadsafe(q.put(line.rstrip("\r\n")), loop)
                p.wait()
                asyncio.run_coroutine_threadsafe(q.put(None), loop)

            threading.Thread(target=reader, daemon=True).start()

            while True:
                line = await q.get()
                if line is None:
                    break
                logger.debug("stdout: %s", line.strip())
                try:
                    if progress_store is not None:
                        progress_store.append_log(task_id, line)
                except Exception:
                    pass
                await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": line})
                if line.startswith("PROGRESS "):
                    try:
                        pval = float(line.split(" ", 1)[1])
                        try:
                            if progress_store is not None:
                                progress_store.set(task_id, {"percent": max(0.0, min(100.0, pval))})
                        except Exception:
                            pass
                        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": pval})
                    except Exception:
                        pass
                else:
                    p = _parse_progress(line)
                    if p >= 0:
                        try:
                            if progress_store is not None:
                                progress_store.set(task_id, {"percent": p})
                        except Exception:
                            pass
                        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": p})

        # Mark done only on success path
        try:
            if progress_store is not None:
                progress_store.set(task_id, {"stage": "done", "percent": 100})
        except Exception:
            pass
        await _broadcast(task_id, {"type": "done", "task_id": task_id, "percent": 100.0})
    except Exception as e:  # pragma: no cover
        logger.exception("task %s failed", task_id)
        try:
            # Record error into log and set strict error stage
            try:
                if progress_store is not None:
                    progress_store.append_log(task_id, f"[error] {e}")
                    progress_store.set(task_id, {"stage": "error", "percent": 100})
            except Exception:
                pass
            await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": f"[error] {e}"})
            await _broadcast(task_id, {"type": "error", "task_id": task_id, "message": str(e)})
        finally:
            # Do not mark done on failures


@router.post("/swing/generate")
async def swing_generate(payload: Dict[str, Any]):
    symbol = payload.get("symbol")
    timeframe = payload.get("timeframe")
    algo = payload.get("algo", "zigzag")
    params = payload.get("params", {}) or {}
    batch_size = int(payload.get("batch_size") or 0)
    overlap = int(payload.get("overlap") or 0)
    dry_run = bool(payload.get("dry_run") or False)

    # 1) 取得/產生 taskId（支援前端傳入）
    task_id = payload.get("taskId") or uuid.uuid4().hex

    # 2) 立刻初始化進度，避免前端查 snapshot 出現 404
    try:
        if progress_store is not None:
            progress_store.set(task_id, {"stage": "started", "percent": 0, "logs": ["task created"]})
    except Exception:
        pass
    subscribers[task_id] = set()

    # 3) 啟動背景任務（不要阻塞回應）
    if symbol and timeframe:
        asyncio.create_task(
            _run_task_and_stream(
                task_id,
                symbol=symbol,
                timeframe=timeframe,
                algo=algo,
                params=params,
                batch_size=batch_size,
                overlap=overlap,
                # pass-through dry-run via params -> CLI --dry-run
            )
        )
    else:
        # No specific symbol/timeframe provided -> run entire matrix using default config
        config_path = str((_get_project_root() / "configs" / "swing_matrix.yaml").resolve())
        asyncio.create_task(_run_task_and_stream(task_id, config_path=config_path))
    # 4) 立即回應（舊前端使用 task_id，雙欄位回傳以相容）
    return {"taskId": task_id, "task_id": task_id}


# LEGACY (disabled): superseded by src.backend.swing_progress_router
# @router.websocket("/swing/_legacy_progress/{task_id}")
async def swing_progress_ws(websocket: WebSocket, task_id: str):
    return


# LEGACY (disabled): superseded by src.backend.swing_progress_router
# @router.get("/swing/_legacy_progress/{task_id}/snapshot")
async def swing_progress_snapshot(task_id: str):
    return {"legacy": True}


@router.get("/swing/matrix")
async def get_swing_matrix():
    """Return matrix YAML as JSON."""
    root = _get_project_root()
    cfg = root / "configs" / "swing_matrix.yaml"
    if not cfg.exists():
        logger.warning("matrix not found at %s", cfg)
        raise HTTPException(status_code=404, detail="matrix not found")
    try:
        import yaml  # type: ignore
        with cfg.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/swing/generate-matrix")
async def swing_generate_matrix(payload: Dict[str, Any] = None):
    """Launch swing generation for entire matrix (or specified config path)."""
    payload = payload or {}
    config_path = payload.get("config") or str((_get_project_root() / "configs" / "swing_matrix.yaml").resolve())

    # 支援外部自帶 taskId
    task_id = payload.get("taskId") or uuid.uuid4().hex
    try:
        if progress_store is not None:
            progress_store.set(task_id, {"stage": "started", "percent": 0, "logs": ["task created"]})
    except Exception:
        pass
    subscribers[task_id] = set()
    asyncio.create_task(_run_task_and_stream(task_id, config_path=config_path))
    return {"taskId": task_id, "task_id": task_id}


