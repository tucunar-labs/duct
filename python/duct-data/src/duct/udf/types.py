"""Mapping between Python annotations, DuckDB type names and Arrow types."""

from __future__ import annotations

import importlib
from typing import Any

# DuckDB type name -> pyarrow factory name. Only types the Go side can marshal.
_ARROW: dict[str, str] = {
    "BOOLEAN": "bool_",
    "TINYINT": "int8",
    "SMALLINT": "int16",
    "INTEGER": "int32",
    "BIGINT": "int64",
    "FLOAT": "float32",
    "DOUBLE": "float64",
    "VARCHAR": "string",
}

_ALIASES = {
    "INT": "INTEGER",
    "STRING": "VARCHAR",
    "TEXT": "VARCHAR",
    "BOOL": "BOOLEAN",
    "REAL": "FLOAT",
    "INT1": "TINYINT",
    "INT2": "SMALLINT",
    "INT4": "INTEGER",
    "INT8": "BIGINT",
    "FLOAT4": "FLOAT",
    "FLOAT8": "DOUBLE",
}

# Lowercase builtin names are Python annotations (``int`` is 64-bit); UPPERCASE is DuckDB.
_PY = {"int": "BIGINT", "float": "DOUBLE", "str": "VARCHAR", "bool": "BOOLEAN"}

SUPPORTED = tuple(_ARROW)


def duckdb_type(name: str | None) -> str | None:
    """Resolve a Python builtin (``int``) or a DuckDB type name (``INTEGER``, ``DOUBLE``).

    Returns ``None`` when it is not one of the supported scalar types.
    """
    if name is None:
        return None
    n = "|".join(x.strip() for x in name.split("|") if x.strip() != "None").strip()
    if n in _PY:
        return _PY[n]
    up = n.upper()
    up = _ALIASES.get(up, up)
    return up if up in _ARROW else None


def arrow_type(duck: str) -> Any:
    """Return the ``pyarrow`` type for a supported DuckDB type name."""
    pa = importlib.import_module("pyarrow")
    return getattr(pa, _ARROW[duck])()
