"""Client connection to ``duct serve``.

Two transports, picked from the URL scheme:

* ``http://`` / ``https://``: the HTTP protocol. With ``pyarrow`` installed
  (``duct-data[arrow]``) results arrive as ``FORMAT ArrowStream`` and stay columnar;
  without it the connection falls back to ``FORMAT JSON`` (stdlib only).
* ``postgres://`` / ``postgresql://``: the Postgres wire protocol through ``psycopg``
  (``duct-data[pg]``). Duct logs in with user and password: pass the password as
  ``token=`` (or ``DUCT_TOKEN``) if the URL carries none.

Neither transport ever sees lake credentials: the server runs every statement as the
authenticated user, over that user's role views.
"""

from __future__ import annotations

import importlib
import json
import os
import urllib.error
import urllib.request
from collections.abc import Sequence
from typing import Any, Literal, Self

ENV_URL = "DUCT_URL"
ENV_TOKEN = "DUCT_TOKEN"

Format = Literal["auto", "arrow", "json"]


class DuctError(RuntimeError):
    """Raised when the server rejects a query or cannot be reached."""


def _missing(extra: str, module: str) -> ImportError:
    return ImportError(
        f"{module} is required for this call; install it with: pip install 'duct-data[{extra}]'"
    )


def _import(extra: str, module: str) -> Any:
    try:
        return importlib.import_module(module)
    except ImportError:
        raise _missing(extra, module) from None


class Result:
    """Materialized query result: column names plus rows (a dict per row).

    Results from the Arrow and Postgres transports keep an Arrow table, so
    :meth:`to_arrow`, :meth:`to_pandas` and :meth:`to_polars` do not go through Python rows.
    """

    def __init__(
        self, columns: Sequence[str], rows: list[dict[str, Any]], *, table: Any = None
    ) -> None:
        self.columns: list[str] = list(columns)
        self._rows: list[dict[str, Any]] | None = rows
        self._table: Any = table

    @classmethod
    def from_arrow(cls, table: Any) -> Result:
        """Wrap a ``pyarrow.Table``; ``rows`` is built lazily."""
        res = cls(list(table.column_names), [], table=table)
        res._rows = None  # pyright: ignore[reportPrivateUsage]
        return res

    @property
    def rows(self) -> list[dict[str, Any]]:
        if self._rows is None:
            self._rows = self._table.to_pylist()
        return self._rows or []

    def __len__(self) -> int:
        return int(self._table.num_rows) if self._rows is None else len(self._rows)

    def to_arrow(self) -> Any:
        """Return a ``pyarrow.Table`` (needs ``duct-data[arrow]``)."""
        if self._table is not None:
            return self._table
        pa = _import("arrow", "pyarrow")
        if not self.rows:
            return pa.table({c: [] for c in self.columns})
        return pa.Table.from_pylist(self.rows)

    def to_pandas(self) -> Any:
        """Return a ``pandas.DataFrame`` (needs ``duct-data[pandas]``)."""
        pd = _import("pandas", "pandas")
        if self._table is not None:
            return self._table.to_pandas()
        return pd.DataFrame(self.rows, columns=self.columns)

    def to_polars(self) -> Any:
        """Return a ``polars.DataFrame`` (needs ``duct-data[polars]``)."""
        pl = _import("polars", "polars")
        if self._table is not None:
            return pl.from_arrow(self._table)
        return pl.DataFrame(self.rows, schema=self.columns)


class Connection:
    """Base class of the connections returned by :func:`connect`."""

    url: str

    def sql(self, query: str) -> Result:
        """Run ``query`` and return its rows."""
        raise NotImplementedError

    def close(self) -> None:
        """Release the connection (idempotent)."""

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class HTTPConnection(Connection):
    """HTTP connection to ``duct serve``. Prefer :func:`connect`."""

    def __init__(
        self,
        url: str,
        token: str | None = None,
        timeout: float = 60.0,
        format: Format = "auto",
    ) -> None:
        self.url = url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._closed = False
        if format == "auto":
            try:
                importlib.import_module("pyarrow")
                format = "arrow"
            except ImportError:
                format = "json"
        elif format == "arrow":
            _import("arrow", "pyarrow")
        self.format: Format = format

    def sql(self, query: str) -> Result:
        if self._closed:
            raise DuctError("connection is closed")
        fmt = "ArrowStream" if self.format == "arrow" else "JSON"
        body = (query.rstrip().rstrip(";") + f"\nFORMAT {fmt}").encode()
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(f"{self.url}/query", data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                if self.format == "arrow":
                    ipc = _import("arrow", "pyarrow.ipc")
                    return Result.from_arrow(ipc.open_stream(resp).read_all())
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace").strip()
            raise DuctError(f"HTTP {e.code}: {detail}") from None
        except urllib.error.URLError as e:
            raise DuctError(f"cannot reach {self.url}: {e.reason}") from None
        columns = [m["name"] for m in payload.get("meta", [])]
        return Result(columns, list(payload.get("data", [])))

    def close(self) -> None:
        self._closed = True


class PostgresConnection(Connection):
    """Postgres-wire connection to ``duct serve`` (psycopg). Prefer :func:`connect`."""

    def __init__(self, url: str, token: str | None = None, timeout: float = 60.0) -> None:
        psycopg = _import("pg", "psycopg")
        _import("pg", "psycopg.conninfo")
        self.url = url
        kwargs: dict[str, Any] = {"connect_timeout": int(timeout), "autocommit": True}
        if token and not psycopg.conninfo.conninfo_to_dict(url).get("password"):
            kwargs["password"] = token  # the URL's own password wins
        try:
            self._con: Any = psycopg.connect(url, **kwargs)
        except psycopg.Error as e:
            raise DuctError(f"cannot connect: {e}") from None
        self._psycopg = psycopg

    def sql(self, query: str) -> Result:
        if self._con is None or self._con.closed:
            raise DuctError("connection is closed")
        try:
            with self._con.cursor() as cur:
                cur.execute(query.rstrip().rstrip(";"))
                if cur.description is None:
                    return Result([], [])
                columns = [d.name for d in cur.description]
                rows = [dict(zip(columns, r, strict=True)) for r in cur.fetchall()]
        except self._psycopg.Error as e:
            raise DuctError(str(e).strip()) from None
        return Result(columns, rows)

    def close(self) -> None:
        if self._con is not None:
            self._con.close()


def connect(
    url: str | None = None,
    token: str | None = None,
    *,
    format: Format = "auto",
) -> Connection:
    """Connect to ``duct serve``.

    ``url`` and ``token`` default to the ``DUCT_URL`` and ``DUCT_TOKEN`` environment variables.
    The scheme picks the transport (``http(s)://`` or ``postgres(ql)://``); ``format`` only
    applies to HTTP (``"auto"``: Arrow when pyarrow is installed, else JSON).
    """
    url = url or os.environ.get(ENV_URL)
    if not url:
        raise ValueError(f"no URL given: pass url= or set {ENV_URL}")
    token = token or os.environ.get(ENV_TOKEN) or None
    scheme = url.split(":", 1)[0].lower()
    if scheme in ("http", "https"):
        return HTTPConnection(url, token, format=format)
    if scheme in ("postgres", "postgresql"):
        return PostgresConnection(url, token)
    raise ValueError(f"unsupported URL scheme {scheme!r}: use http(s):// or postgres(ql)://")
