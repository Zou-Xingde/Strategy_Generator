from __future__ import annotations

from typing import Optional, Dict, Any
from threading import Lock
from copy import deepcopy
import time


class ProgressStore:
    """Abstract progress store interface."""

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def set(self, task_id: str, data: Dict[str, Any]) -> None:
        raise NotImplementedError

    def append_log(self, task_id: str, msg: str) -> None:
        raise NotImplementedError

    def clear(self, task_id: str) -> None:
        raise NotImplementedError


class InMemoryProgressStore(ProgressStore):
    """
    Thread-safe in-memory progress store.
    Data shape suggestion:
      {
        "stage": str,
        "percent": int,  # 0~100
        "logs": list[str],
        "updated_at": float
      }
    """

    def __init__(self) -> None:
        self._d: Dict[str, Dict[str, Any]] = {}
        self._lock = Lock()

    def get(self, task_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            v = self._d.get(task_id)
            # return a copy to avoid external mutation
            return deepcopy(v) if v is not None else None

    def set(self, task_id: str, data: Dict[str, Any]) -> None:
        with self._lock:
            payload = dict(data)
            payload["updated_at"] = time.time()
            # keep existing logs if caller didn't pass one
            if "logs" not in payload:
                old = self._d.get(task_id)
                if old and isinstance(old.get("logs"), list):
                    payload["logs"] = old["logs"]
                else:
                    payload["logs"] = []
            self._d[task_id] = payload

    def append_log(self, task_id: str, msg: str) -> None:
        with self._lock:
            snap = self._d.setdefault(
                task_id, {"stage": "unknown", "percent": 0, "logs": []}
            )
            logs = snap.setdefault("logs", [])
            logs.append(str(msg))
            snap["updated_at"] = time.time()

    def clear(self, task_id: str) -> None:
        with self._lock:
            self._d.pop(task_id, None)


# Export a singleton store for development (single-process).
# Later you can swap this out for a Redis-backed implementation.
store = InMemoryProgressStore()

__all__ = ["ProgressStore", "InMemoryProgressStore", "store"]


