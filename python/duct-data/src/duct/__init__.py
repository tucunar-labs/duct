"""Duct Python SDK: connect to a lake and declare UDFs and Python tables."""

from duct._connection import (
    Connection,
    DuctError,
    HTTPConnection,
    PostgresConnection,
    Result,
    connect,
)
from duct.registry import Expectation, FunctionSpec, TableSpec, function, table

__all__ = [
    "Connection",
    "DuctError",
    "Expectation",
    "FunctionSpec",
    "HTTPConnection",
    "PostgresConnection",
    "Result",
    "TableSpec",
    "connect",
    "function",
    "table",
]
__version__ = "0.0.1"
