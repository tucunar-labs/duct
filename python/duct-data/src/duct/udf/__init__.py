"""UDF runtime: the worker process that runs ``@duct.function`` code for the Go core.

``python -m duct.udf.worker --load functions/`` speaks a framed Arrow IPC protocol over
stdin/stdout (see :mod:`duct.udf.worker`). Nothing here talks to the lake.
"""

from duct.udf.types import arrow_type, duckdb_type

__all__ = ["arrow_type", "duckdb_type"]
