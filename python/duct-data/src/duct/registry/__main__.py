"""CLI: ``python -m duct.registry [--json] [--load PATH ...]``."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

from duct import registry


def _load(path: Path) -> None:
    files = sorted(path.glob("*.py")) if path.is_dir() else [path]
    for f in files:
        spec = importlib.util.spec_from_file_location(f"_duct_user_{f.stem}", f)
        if spec is None or spec.loader is None:
            raise SystemExit(f"cannot load {f}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m duct.registry")
    p.add_argument("--json", action="store_true", help="print machine-readable JSON")
    p.add_argument("--load", action="append", default=[], type=Path, help="file or dir to import")
    args = p.parse_args(argv)
    for path in args.load:
        _load(path)
    if args.json:
        print(json.dumps(registry.to_dict(), indent=2))
    else:
        for f in registry.functions():
            print(f"function {f.name}({', '.join(f.input_types)}) -> {f.return_type}")
        for t in registry.tables():
            print(f"table {t.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
