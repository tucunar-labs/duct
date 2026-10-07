"""Client connection to ``duct serve``.

Only the HTTP path exists for now. TODO: Arrow (``FORMAT ArrowStream``) and
Postgres wire (``duct-data[pg]``) transports.
"""

from __future__ import annotations

import importlib
import json
import os
import urllib.error
import urllib.request
from collections.abc import Sequence
from typing import Any, Self


class DuctError(RuntimeError):
    """Raised when the server rejects a query or cannot be reached."""


def _missing(extra: str, module: str) -> ImportError:
    return ImportError(
        f"{module} is required for this call; install it with: pip install 'duct-data[{extra}]'"
    )


class Result:
    """Materialized query result (column names plus row dicts)."""

    def __init__(self, columns: Sequence[str], rows: list[dict[str, Any]]) -> None:
        self.columns: list[str] = list(columns)
        self.rows: list[dict[str, Any]] = rows

    def __len__(self) -> int:
        return len(self.rows)

    def to_arrow(self) -> Any:
        """Return a ``pyarrow.Table`` (needs ``duct-data[arrow]``)."""
        try:
            pa = importlib.import_module("pyarrow")
        except ImportError:
            raise _missing("arrow", "pyarrow") from None
        if not self.rows:
            return pa.table({c: [] for c in self.columns})
        return pa.Table.from_pylist(self.rows)

    def to_pandas(self) -> Any:
        """Return a ``pandas.DataFrame`` (needs ``duct-data[pandas]``)."""
        try:
            pd = importlib.import_module("pandas")
        except ImportError:
            raise _missing("pandas", "pandas") from None
        return pd.DataFrame(self.rows, columns=self.columns)

    def to_polars(self) -> Any:
        """Return a ``polars.DataFrame`` (needs ``duct-data[polars]``)."""
        try:
            pl = importlib.import_module("polars")
        except ImportError:
            raise _missing("polars", "polars") from None
        return pl.DataFrame(self.rows, schema=self.columns)


class Connection:
    """HTTP connection to ``duct serve``. Prefer :func:`connect`."""

    def __init__(self, url: str, token: str | None = None, timeout: float = 60.0) -> None:
        self.url = url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._closed = False

    def sql(self, query: str) -> Result:
        """Run ``query`` and return its rows."""
        if self._closed:
            raise DuctError("connection is closed")
        # TODO: switch to FORMAT ArrowStream when pyarrow is available.
        body = (query.rstrip().rstrip(";") + "\nFORMAT JSON").encode()
        headers = {"Content-Type": "text/plain; charset=utf-8"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(f"{self.url}/query", data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                payload = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace").strip()
            raise DuctError(f"HTTP {e.code}: {detail}") from None
        except urllib.error.URLError as e:
            raise DuctError(f"cannot reach {self.url}: {e.reason}") from None
        columns = [m["name"] for m in payload.get("meta", [])]
        return Result(columns, list(payload.get("data", [])))

    def close(self) -> None:
        """Release the connection (idempotent)."""
        self._closed = True

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def connect(url: str | None = None, token: str | None = None) -> Connection:
    """Connect to ``duct serve``.

    ``url`` and ``token`` default to the ``DUCT_URL`` and ``DUCT_TOKEN`` environment variables.
    """
    url = url or os.environ.get("DUCT_URL")
    if not url:
        raise ValueError("no URL given: pass url= or set DUCT_URL")
    token = token or os.environ.get("DUCT_TOKEN") or None
    return Connection(url, token)
