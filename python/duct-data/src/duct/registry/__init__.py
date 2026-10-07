"""In-process registry of user-declared UDFs and Python tables.

The Go core runs ``python -m duct.registry --json --load <path>`` to discover them.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any, Literal, overload

from duct.udf.types import duckdb_type

SCHEMA_VERSION = 2

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
    # annotations; pass ``types=``/``returns=`` to the decorator). Schema version 2.
    duckdb_args: list[str | None]
    duckdb_returns: str | None
    fn: Callable[..., Any] = field(repr=False, compare=False)


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
    BIGINT, ``float`` DOUBLE, ``str`` VARCHAR, ``bool`` BOOLEAN) or, always needed for
    vectorized functions, from ``types=["DOUBLE", ...]`` and ``returns="DOUBLE"``.
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
            duck_in = [duckdb_type(_type_name(p.annotation)) for p in sig.parameters.values()]
        duck_out = duckdb_type(returns or _type_name(sig.return_annotation))
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
