from collections.abc import Iterator

import pytest

from duct import registry


@pytest.fixture(autouse=True)
def clean_registry() -> Iterator[None]:
    registry.clear()
    yield
    registry.clear()
