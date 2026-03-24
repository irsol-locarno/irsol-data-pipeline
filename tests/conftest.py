from pathlib import Path

import pytest

from irsol_data_pipeline.logging_config import setup_logging


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session", autouse=True)
def setup_test_logging() -> None:
    """Configure logging for tests."""
    setup_logging(level="DEBUG")
