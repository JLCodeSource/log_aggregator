import pytest
from aggregator import __version__


@pytest.mark.unit
def test_version() -> None:
    assert __version__ == "0.1.0"
