from __future__ import annotations

import asyncio
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException

from src.backend.progress_store import store


router = APIRouter(prefix="/swing", tags=["swing"])


@router.get("/progress/{task_id}/snapshot")
def get_snapshot(task_id: str):
    snap = store.get(task_id)
    if not snap:
        raise HTTPException(status_code=404, detail="not found")
    stage = snap.get("stage") if isinstance(snap, dict) else None
    try:
        percent = int(snap.get("percent", 0)) if isinstance(snap, dict) else 0
    except Exception:
        percent = 0
    logs = snap.get("logs", []) if isinstance(snap, dict) else []

    # Merge new format with legacy-compatible fields
    merged = dict(snap)
    merged.update({
        "task_id": task_id,
        "done": (percent >= 100) or (stage == "done"),
        "lines": logs,
    })
    return merged


@router.websocket("/progress/{task_id}")
async def progress_ws(ws: WebSocket, task_id: str):
    await ws.accept()
    try:
        last: dict | None = None
        while True:
            snap = store.get(task_id)
            if snap != last:
                await ws.send_json(snap or {"stage": "unknown", "percent": 0, "logs": []})
                last = snap
            await asyncio.sleep(0.5)
    except WebSocketDisconnect:
        return


@router.get("/_store_id")
def get_store_id():
    return {"id": id(store)}


@router.post("/dev/start")
async def dev_start():
    """Development helper endpoint that starts a fake progressing task.

    Useful for validating WS and HTTP polling without running the real generator.
    """
    import uuid

    task_id = uuid.uuid4().hex
    store.set(task_id, {"stage": "started", "percent": 0, "logs": ["dev task created"]})

    async def run() -> None:
        for p, st in [(10, "load-data"), (35, "preprocess"), (70, "swing-calc"), (90, "db-import"), (100, "done")]:
            snap = store.get(task_id) or {"logs": []}
            logs = list(snap.get("logs", []))
            logs.append(f"stage -> {st}")
            store.set(task_id, {"stage": st, "percent": p, "logs": logs})
            await asyncio.sleep(0.6)

    asyncio.create_task(run())
    return {"taskId": task_id}


