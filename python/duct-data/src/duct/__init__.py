"""Duct Python SDK: connect to a lake and declare UDFs and Python tables."""

from duct._connection import Connection, Result, connect
from duct.registry import Expectation, FunctionSpec, TableSpec, function, table

__all__ = [
    "Connection",
    "Expectation",
    "FunctionSpec",
    "Result",
    "TableSpec",
    "connect",
    "function",
    "table",
]
__version__ = "0.0.1"
