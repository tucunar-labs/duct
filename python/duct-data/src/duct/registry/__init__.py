"""In-process registry of user-declared UDFs and Python tables.

The Go core runs ``python -m duct.registry --json --load <path>`` to discover them.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, overload

from duct.udf.types import duckdb_type

SCHEMA_VERSION = 3

OnViolation = Literal["drop_row", "fail_update"]


@dataclass(frozen=True)
class Expectation:
    """Data quality rule: ``CONSTRAINT name EXPECT (expr) ON VIOLATION ...``."""

    name: str
    expr: str
    on_violation: OnViolation = "fail_update"


@dataclass(frozen=True)
class FunctionSpec:
    """Metadata of a UDF declared with :func:`function`."""

    name: str
    module: str
    qualname: str
    input_types: list[str]
    return_type: str | None
    vectorized: bool
    # DuckDB types the function is registered with (None = could not be resolved from the
    # annotations; pass ``types=``/``returns=`` to the decorator).
    duckdb_args: list[str | None]
    duckdb_returns: str | None
    fn: Callable[..., Any] = field(repr=False, compare=False)
    # "scalar" (``@function``) or "table" (``@table_function``: ``columns`` is its declared
    # schema as ``[name, duckdb type]`` pairs and ``duckdb_returns`` is None).
    kind: str = "scalar"
    columns: list[list[str]] = field(default_factory=lambda: [])


@dataclass(frozen=True)
class TableSpec:
    """Metadata of a Python table builder declared with :func:`table`."""

    name: str
    module: str
    qualname: str
    expectations: list[Expectation]
    fn: Callable[..., Any] = field(repr=False, compare=False)


_functions: dict[str, FunctionSpec] = {}
_tables: dict[str, TableSpec] = {}


def _type_name(annotation: Any) -> str | None:
    if annotation is inspect.Signature.empty:
        return None
    if isinstance(annotation, str):
        return annotation
    return getattr(annotation, "__name__", None) or str(annotation)


def _annotation(annotation: Any) -> Any:
    """The annotation as ``duckdb_type`` wants it: ``None`` when absent."""
    return None if annotation is inspect.Signature.empty else annotation


def _register[T](registry: dict[str, T], name: str, spec: T) -> None:
    if name in registry:
        raise ValueError(f"duplicate registration: {name!r}")
    registry[name] = spec


@overload
def function[F: Callable[..., Any]](fn: F, /) -> F: ...
@overload
def function[F: Callable[..., Any]](
    *,
    name: str | None = None,
    vectorized: bool = False,
    types: Sequence[str] | None = None,
    returns: str | None = None,
) -> Callable[[F], F]: ...
def function(
    fn: Callable[..., Any] | None = None,
    /,
    *,
    name: str | None = None,
    vectorized: bool = False,
    types: Sequence[str] | None = None,
    returns: str | None = None,
) -> Any:
    """Register a Python UDF. Usable bare (``@duct.function``) or with options.

    ``vectorized=True`` means the function receives and returns Arrow arrays (one per
    argument, all of the same length). Otherwise it is called once per row, with NULL in
    giving NULL out. Argument and result types come from the annotations (``int`` is
    BIGINT, ``float`` DOUBLE, ``str`` VARCHAR, ``bool`` BOOLEAN, ``datetime.date`` DATE,
    ``datetime.datetime`` TIMESTAMP, ``list[int]`` BIGINT[]) or, always needed for
    vectorized functions, from ``types=["DOUBLE", ...]`` and ``returns="DOUBLE"``.
    ``DECIMAL(p,s)`` and ``STRUCT(a BIGINT, b VARCHAR)`` have no annotation: use ``types=``/
    ``returns=``. Scalar functions receive ``datetime.date``/``datetime``/``Decimal``/``list``/
    ``dict``; vectorized ones receive the matching Arrow arrays.
    """

    def wrap(f: Callable[..., Any]) -> Callable[..., Any]:
        sig = inspect.signature(f)
        inputs = [_type_name(p.annotation) or "unknown" for p in sig.parameters.values()]
        variadic = any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in sig.parameters.values())
        if types is not None and not variadic and len(types) != len(inputs):
            raise ValueError(f"{f.__name__}: types= has {len(types)} items for {len(inputs)} args")
        if types is not None:
            inputs = list(types)
            duck_in = [duckdb_type(t) for t in types]
        else:
            duck_in = [duckdb_type(_annotation(p.annotation)) for p in sig.parameters.values()]
        duck_out = duckdb_type(returns or _annotation(sig.return_annotation))
        spec = FunctionSpec(
            name=name or f.__name__,
            module=f.__module__,
            qualname=f.__qualname__,
            input_types=inputs,
            return_type=_type_name(sig.return_annotation),
            vectorized=vectorized,
            duckdb_args=duck_in,
            duckdb_returns=duck_out,
            fn=f,
        )
        _register(_functions, spec.name, spec)
        return f

    return wrap(fn) if fn is not None else wrap


def table_function[F: Callable[..., Any]](
    *,
    columns: Mapping[str, str] | Sequence[tuple[str, str]],
    name: str | None = None,
    types: Sequence[str] | None = None,
) -> Callable[[F], F]:
    """Register a Python table function: ``SELECT * FROM squares(10)`` (ADR 0005).

    ``columns`` declares the result schema (``{"n": "BIGINT", "sq": "BIGINT"}``). The
    arguments are constants of the query; their types come from the annotations or
    ``types=[...]``. The function is called once and returns the rows as a ``pyarrow.Table`` /
    ``RecordBatch``, an iterable of them, a ``{column: values}`` dict or a list of row dicts;
    columns are matched by name and cast to the declared types.
    """
    cols = [[n, t] for n, t in (columns.items() if isinstance(columns, Mapping) else columns)]
    if not cols:
        raise ValueError("table_function: columns= needs at least one column")
    resolved: list[list[str]] = []
    for n, t in cols:
        d = duckdb_type(t)
        if d is None:
            raise ValueError(f"table_function: column {n!r} has unsupported type {t!r}")
        resolved.append([n, d])

    def wrap(f: F) -> F:
        sig = inspect.signature(f)
        inputs = [_type_name(p.annotation) or "unknown" for p in sig.parameters.values()]
        if types is not None:
            if len(types) != len(inputs):
                raise ValueError(
                    f"{f.__name__}: types= has {len(types)} items for {len(inputs)} args"
                )
            inputs = list(types)
            duck_in = [duckdb_type(t) for t in types]
        else:
            duck_in = [duckdb_type(_annotation(p.annotation)) for p in sig.parameters.values()]
        spec = FunctionSpec(
            name=name or f.__name__,
            module=f.__module__,
            qualname=f.__qualname__,
            input_types=inputs,
            return_type=None,
            vectorized=False,
            duckdb_args=duck_in,
            duckdb_returns=None,
            fn=f,
            kind="table",
            columns=resolved,
        )
        _register(_functions, spec.name, spec)
        return f

    return wrap


def table[F: Callable[..., Any]](
    *, name: str | None = None, expectations: Iterable[Expectation] = ()
) -> Callable[[F], F]:
    """Register a Python table builder (``@duct.table(name=..., expectations=[...])``)."""

    def wrap(f: F) -> F:
        spec = TableSpec(
            name=name or f.__name__,
            module=f.__module__,
            qualname=f.__qualname__,
            expectations=list(expectations),
            fn=f,
        )
        _register(_tables, spec.name, spec)
        return f

    return wrap


def functions() -> list[FunctionSpec]:
    """Registered UDFs, in declaration order."""
    return list(_functions.values())


def tables() -> list[TableSpec]:
    """Registered tables, in declaration order."""
    return list(_tables.values())


def clear() -> None:
    """Forget everything (for tests)."""
    _functions.clear()
    _tables.clear()


def to_dict() -> dict[str, Any]:
    """JSON-serializable snapshot consumed by the Go core."""
    return {
        "schema_version": SCHEMA_VERSION,
        "functions": [
            {k: v for k, v in asdict(s).items() if k != "fn"} for s in _functions.values()
        ],
        "tables": [{k: v for k, v in asdict(s).items() if k != "fn"} for s in _tables.values()],
    }
