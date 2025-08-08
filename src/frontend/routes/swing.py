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


# In-memory stores for progress and websocket subscribers
progress_store: Dict[str, Dict[str, Any]] = {}
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
    script_path = root / "scripts" / "swing_generate.py"
    if not script_path.exists():
        # Fail fast and notify
        progress_store[task_id] = {"percent": 0.0, "log": ["script not found"], "done": True}
        await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": "script not found"})
        await _broadcast(task_id, {"type": "done", "task_id": task_id, "percent": 100.0})
        return

    # Ensure structures exist
    progress_store.setdefault(task_id, {"percent": 0.0, "log": [], "done": False})
    subscribers.setdefault(task_id, set())

    try:
        # Build CLI
        args = [sys.executable, str(script_path)]
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
        # Optional dry-run propagated via params flag 'dry_run': true
        try:
            if params and isinstance(params, dict) and params.get("dry_run"):
                args += ["--dry-run"]
        except Exception:
            pass

        logger.info("CLI: %s", args)
        # Announce start to clients
        progress_store[task_id]["log"].append(f"[server] start task {task_id}")
        await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": f"[server] start task {task_id}"})
        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": float(progress_store[task_id]["percent"])})

        try:
            # Preferred async subprocess (may fail on some Windows loop policies)
            proc = await asyncio.create_subprocess_exec(
                *args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(root),
            )

            assert proc.stdout is not None

            async for raw in proc.stdout:
                try:
                    line = raw.decode(errors="ignore").rstrip('\r\n')
                except Exception:
                    line = ""

                logger.debug("stdout: %s", line.strip())

                # Append log
                entry = progress_store[task_id]
                entry["log"].append(line)
                await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": line})

                # Prefer explicit PROGRESS marker
                if line.startswith("PROGRESS "):
                    try:
                        pval = float(line.split(" ", 1)[1])
                        entry["percent"] = max(0.0, min(100.0, pval))
                        await _broadcast(
                            task_id,
                            {"type": "progress", "task_id": task_id, "percent": entry["percent"]},
                        )
                    except Exception:
                        pass
                else:
                    p = _parse_progress(line)
                    if p >= 0:
                        entry["percent"] = p
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
                entry = progress_store[task_id]
                entry["log"].append(line)
                await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": line})
                if line.startswith("PROGRESS "):
                    try:
                        pval = float(line.split(" ", 1)[1])
                        entry["percent"] = max(0.0, min(100.0, pval))
                        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": entry["percent"]})
                    except Exception:
                        pass
                else:
                    p = _parse_progress(line)
                    if p >= 0:
                        entry["percent"] = p
                        await _broadcast(task_id, {"type": "progress", "task_id": task_id, "percent": p})

        # Mark done
        progress_store[task_id]["done"] = True
        progress_store[task_id]["percent"] = 100.0
        await _broadcast(task_id, {"type": "done", "task_id": task_id, "percent": 100.0})
    except Exception as e:  # pragma: no cover
        logger.exception("task %s failed", task_id)
        try:
            # Record error into log and notify clients
            entry = progress_store.setdefault(task_id, {"percent": 0.0, "log": [], "done": False})
            entry["log"].append(f"[error] {e}")
            await _broadcast(task_id, {"type": "log", "task_id": task_id, "line": f"[error] {e}"})
            await _broadcast(task_id, {"type": "error", "task_id": task_id, "message": str(e)})
        finally:
            # ensure done flag to unblock listeners
            progress_store[task_id]["done"] = True


@router.post("/swing/generate")
async def swing_generate(payload: Dict[str, Any]):
    symbol = payload.get("symbol")
    timeframe = payload.get("timeframe")
    algo = payload.get("algo", "zigzag")
    params = payload.get("params", {}) or {}
    batch_size = int(payload.get("batch_size") or 0)
    overlap = int(payload.get("overlap") or 0)
    dry_run = bool(payload.get("dry_run") or False)

    task_id = uuid.uuid4().hex
    # Initialize store
    progress_store[task_id] = {"percent": 0.0, "log": [], "done": False}
    subscribers[task_id] = set()

    # Fire-and-forget background task
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
    return {"task_id": task_id}


@router.websocket("/swing/progress/{task_id}")
async def swing_progress_ws(websocket: WebSocket, task_id: str):
    # 允許來自任意 Origin 的升級，並記錄握手請求頭方便排查
    try:
        logger.info("WS connect task_id=%s origin=%s host=%s", task_id, websocket.headers.get("origin"), websocket.headers.get("host"))
        await websocket.accept()
    except Exception as e:
        logger.exception("WS accept failed for task_id=%s: %s", task_id, e)
        return

    subscribers.setdefault(task_id, set()).add(websocket)

    # On join, send snapshot if exists
    snap = progress_store.get(task_id)
    if snap:
        # Send historical logs (trim to last 200 lines to avoid flooding)
        history: List[str] = snap.get("log", [])[-200:]
        if history:
            await websocket.send_json({"type": "log_batch", "task_id": task_id, "lines": history})
        await websocket.send_json({"type": "progress", "task_id": task_id, "percent": float(snap.get("percent", 0.0))})
        if snap.get("done"):
            await websocket.send_json({"type": "done", "task_id": task_id, "percent": float(snap.get("percent", 100.0))})

    try:
        # Keep the connection open;我們不期待客戶端主動發送任何訊息
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        logger.info("WS disconnect task_id=%s", task_id)
    finally:
        # Cleanup subscription
        try:
            subscribers.get(task_id, set()).discard(websocket)
        except Exception:
            pass


@router.get("/swing/progress/{task_id}/snapshot")
async def swing_progress_snapshot(task_id: str):
    """HTTP fallback: return current progress snapshot for polling.
    Never 404 to avoid client console spam; return empty snapshot instead.
    """
    snap = progress_store.get(task_id)
    if not snap:
        # Initialize an empty snapshot to be gentle with polling clients
        progress_store[task_id] = {"percent": 0.0, "log": [], "done": False}
        snap = progress_store[task_id]
    # Return a trimmed log to avoid large payloads
    history: List[str] = snap.get("log", [])[-200:]
    return {
        "task_id": task_id,
        "percent": float(snap.get("percent", 0.0)),
        "done": bool(snap.get("done", False)),
        "lines": history,
    }


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

    task_id = uuid.uuid4().hex
    progress_store[task_id] = {"percent": 0.0, "log": [], "done": False}
    subscribers[task_id] = set()
    asyncio.create_task(_run_task_and_stream(task_id, config_path=config_path))
    return {"task_id": task_id}


