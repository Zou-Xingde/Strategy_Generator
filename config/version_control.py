from __future__ import annotations

from typing import Any, Dict


_ZIGZAG_DEFAULTS: Dict[str, Any] = {
    "depth": 12,
    "deviation": 5.0,
    "backstep": 3,
}

_ALIAS_MAP: Dict[str, str] = {
    # Normalize various zigzag family names to a canonical key
    "zigzag": "zigzag",
    "zigzag_improved": "zigzag",
    "zigzag-improved": "zigzag",
    "zigzag_improved_fixed": "zigzag",
    "zigzag-improved-fixed": "zigzag",
    "zigzag_fixed": "zigzag",
    "zigzag-fixed": "zigzag",
}


def get_algorithm_parameters(algo: str) -> Dict[str, Any]:
    """
    Return default parameters for the given swing algorithm.

    - Tolerant to unknown names; never raises, always returns safe defaults.
    - Currently supports the zigzag-family algorithms.

    Parameters
    ----------
    algo : str
        Algorithm name (e.g., "zigzag").

    Returns
    -------
    Dict[str, Any]
        Parameter dictionary for the algorithm.
    """
    name: str = (algo or "").strip().lower()
    normalized: str = _ALIAS_MAP.get(name, "zigzag")
    if normalized == "zigzag":
        return dict(_ZIGZAG_DEFAULTS)
    # Fallback to zigzag defaults for any unknown or unsupported algorithms
    return dict(_ZIGZAG_DEFAULTS)


def get_version_description() -> str:
    """
    Provide a short human-readable description for the current parameter version.

    Returns
    -------
    str
        Version descriptor string.
    """
    return "v1-zigzag-defaults(depth=12,deviation=5.0,backstep=3)"


if __name__ == "__main__":  # smoke test (no side effects on import)
    print(get_algorithm_parameters("zigzag"))
    print(get_version_description())


