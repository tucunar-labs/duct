"""Mapping between Python annotations, DuckDB type names and Arrow types.

Grammar (the same as the Go core, ``contract.ParseSQLType``): ``BOOLEAN TINYINT SMALLINT
INTEGER BIGINT FLOAT DOUBLE VARCHAR DATE TIMESTAMP`` (microseconds, no zone),
``DECIMAL(p,s)``, lists ``T[]`` and ``STRUCT(name T, ...)``. :func:`duckdb_type` returns the
canonical spelling that the Go side parses.
"""

from __future__ import annotations

import datetime
import importlib
import types as _types
import typing
from dataclasses import dataclass
from typing import Any

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
    "NUMERIC": "DECIMAL",
}

_SCALARS = (
    "BOOLEAN",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "VARCHAR",
    "DATE",
    "TIMESTAMP",
)

# Lowercase builtin names are Python annotations (``int`` is 64-bit); UPPERCASE is DuckDB.
_PY = {
    "int": "BIGINT",
    "float": "DOUBLE",
    "str": "VARCHAR",
    "bool": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP",
}
_PY_OBJ: dict[Any, str] = {
    int: "BIGINT",
    float: "DOUBLE",
    str: "VARCHAR",
    bool: "BOOLEAN",
    datetime.date: "DATE",
    datetime.datetime: "TIMESTAMP",
}

SUPPORTED = (*_SCALARS, "DECIMAL", "LIST", "STRUCT")


@dataclass(frozen=True)
class SQLType:
    """A parsed type: ``kind`` is a scalar name, ``DECIMAL``, ``LIST`` or ``STRUCT``."""

    kind: str
    width: int = 0
    scale: int = 0
    elem: SQLType | None = None
    fields: tuple[tuple[str, SQLType], ...] = ()

    def __str__(self) -> str:
        if self.kind == "DECIMAL":
            return f"DECIMAL({self.width},{self.scale})"
        if self.kind == "LIST":
            assert self.elem is not None
            return f"{self.elem}[]"
        if self.kind == "STRUCT":
            return "STRUCT(" + ", ".join(f"{_ident(n)} {t}" for n, t in self.fields) + ")"
        return self.kind


def _ident(name: str) -> str:
    ok = bool(name) and name.replace("_", "a").isalnum() and not name[0].isdigit()
    return name if ok else '"' + name.replace('"', '""') + '"'


class _Parser:
    def __init__(self, s: str) -> None:
        self.s = s
        self.i = 0

    def _skip(self) -> None:
        while self.i < len(self.s) and self.s[self.i] in " \t":
            self.i += 1

    def _peek(self) -> str:
        self._skip()
        return self.s[self.i] if self.i < len(self.s) else ""

    def _eat(self, c: str) -> bool:
        if self._peek() == c:
            self.i += 1
            return True
        return False

    def _word(self) -> str:
        self._skip()
        st = self.i
        while self.i < len(self.s) and (self.s[self.i] == "_" or self.s[self.i].isalnum()):
            self.i += 1
        return self.s[st : self.i]

    def _number(self) -> int:
        w = self._word()
        if not w.isdigit():
            raise ValueError(f"expected a number at {self.i}")
        return int(w)

    def _field_name(self) -> str:
        if self._peek() == '"':
            self.i += 1
            out = ""
            while self.i < len(self.s):
                c = self.s[self.i]
                self.i += 1
                if c == '"':
                    if self.i < len(self.s) and self.s[self.i] == '"':
                        out += '"'
                        self.i += 1
                        continue
                    return out
                out += c
            raise ValueError("unterminated quoted name")
        n = self._word()
        if not n:
            raise ValueError(f"expected a field name at {self.i}")
        return n

    def parse(self) -> SQLType:
        w = self._word().upper()
        if not w:
            raise ValueError(f"expected a type name at {self.i}")
        w = _ALIASES.get(w, w)
        t: SQLType
        if w == "DECIMAL":
            width, scale = 18, 3
            if self._eat("("):
                width, scale = self._number(), 0
                if self._eat(","):
                    scale = self._number()
                if not self._eat(")"):
                    raise ValueError(f"expected ) at {self.i}")
            if not 1 <= width <= 38 or scale > width:
                raise ValueError(f"DECIMAL({width},{scale}): width 1-38, scale <= width")
            t = SQLType("DECIMAL", width, scale)
        elif w == "STRUCT":
            if not self._eat("("):
                raise ValueError("STRUCT needs (name TYPE, ...)")
            fields: list[tuple[str, SQLType]] = []
            while True:
                name = self._field_name()
                fields.append((name, self.parse()))
                if self._eat(","):
                    continue
                if self._eat(")"):
                    break
                raise ValueError(f"expected , or ) at {self.i}")
            t = SQLType("STRUCT", fields=tuple(fields))
        elif w in _SCALARS:
            t = SQLType(w)
        else:
            raise ValueError(f"unsupported type {w!r}")
        while self._peek() == "[":
            self.i += 1
            if not self._eat("]"):
                raise ValueError(f"expected ] at {self.i}")
            t = SQLType("LIST", elem=t)
        return t


def parse_type(name: str) -> SQLType:
    """Parse a DuckDB type name; raises ``ValueError`` when it is not supported."""
    p = _Parser(name)
    t = p.parse()
    p._skip()  # pyright: ignore[reportPrivateUsage]
    if p.i != len(p.s):
        raise ValueError(f"unexpected {p.s[p.i :]!r}")
    return t


def _split_union(n: str) -> str:
    return "|".join(x.strip() for x in n.split("|") if x.strip() != "None").strip()


def _from_string(name: str) -> str | None:
    n = _split_union(name.strip())
    for prefix in ("datetime.", "typing.", "decimal."):
        if n.startswith(prefix) and n[len(prefix) :] in ("date", "datetime"):
            n = n[len(prefix) :]
    if n in _PY:
        return _PY[n]
    if n.startswith("list[") and n.endswith("]"):
        inner = _from_string(n[5:-1])
        return None if inner is None else inner + "[]"
    if n.startswith("Optional[") and n.endswith("]"):
        return _from_string(n[9:-1])
    try:
        return str(parse_type(n))
    except ValueError:
        return None


def _from_object(a: Any) -> str | None:
    if a in _PY_OBJ:
        return _PY_OBJ[a]
    origin = typing.get_origin(a)
    args = typing.get_args(a)
    if origin in (typing.Union, _types.UnionType):
        rest = [x for x in args if x is not type(None)]
        return _from_object(rest[0]) if len(rest) == 1 else None
    if origin is list and len(args) == 1:
        inner = _from_object(args[0])
        return None if inner is None else inner + "[]"
    return None


def duckdb_type(name: Any) -> str | None:
    """Resolve an annotation (``int``, ``list[int]``, ``datetime.date`` or its string form)
    or a DuckDB type name (``INTEGER``, ``DECIMAL(10,2)``, ``STRUCT(a INT)``) to the canonical
    DuckDB spelling.

    Returns ``None`` when it is not a supported type. ``decimal.Decimal`` and ``dict`` carry no
    precision/fields, so those need an explicit ``types=``/``returns=``.
    """
    if name is None:
        return None
    if isinstance(name, str):
        return _from_string(name)
    return _from_object(name)


def _pa() -> Any:
    return importlib.import_module("pyarrow")


def _arrow(t: SQLType) -> Any:
    pa = _pa()
    simple = {
        "BOOLEAN": "bool_",
        "TINYINT": "int8",
        "SMALLINT": "int16",
        "INTEGER": "int32",
        "BIGINT": "int64",
        "FLOAT": "float32",
        "DOUBLE": "float64",
        "VARCHAR": "string",
        "DATE": "date32",
    }
    if t.kind in simple:
        return getattr(pa, simple[t.kind])()
    if t.kind == "TIMESTAMP":
        return pa.timestamp("us")
    if t.kind == "DECIMAL":
        return pa.decimal128(t.width, t.scale)
    if t.kind == "LIST":
        assert t.elem is not None
        return pa.list_(_arrow(t.elem))
    return pa.struct([pa.field(n, _arrow(ft)) for n, ft in t.fields])


def arrow_type(duck: str) -> Any:
    """Return the ``pyarrow`` type for a supported DuckDB type name."""
    return _arrow(parse_type(duck))
