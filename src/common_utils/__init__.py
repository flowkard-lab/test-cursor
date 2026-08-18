"""Общие утилиты для переиспользования между репозиториями."""

from __future__ import annotations

from typing import Any

__all__ = [
    "drop_old_partitions",
    "get_last_version",
    "get_prev_table_from_ab_view",
]

__version__ = "0.1.0"

_SPARK_EXPORTS = frozenset(__all__)


def __getattr__(name: str) -> Any:
    if name in _SPARK_EXPORTS:
        from common_utils import spark as _spark

        return getattr(_spark, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
