from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def fixture_dir() -> Path:
    """Path to the test fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session", autouse=True)
def setup_test_logging() -> None:
    """Configure logging for tests."""
    from irsol_data_pipeline.logging_config import setup_logging

    setup_logging(level="DEBUG")
