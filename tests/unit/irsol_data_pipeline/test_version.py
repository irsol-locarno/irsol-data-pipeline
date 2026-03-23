"""Tests for package version resolution."""

from __future__ import annotations

import importlib
import importlib.metadata
import sys
from unittest.mock import patch


class TestVersion:
    def test_reads_distribution_version(self) -> None:
        sys.modules.pop("irsol_data_pipeline.version", None)

        with patch(
            "importlib.metadata.version",
            return_value="1.2.3",
        ) as mocked_version:
            module = importlib.import_module("irsol_data_pipeline.version")

        mocked_version.assert_called_once_with("irsol-data-pipeline")
        assert module.__version__ == "1.2.3"

    def test_falls_back_when_distribution_metadata_is_missing(self) -> None:
        sys.modules.pop("irsol_data_pipeline.version", None)

        with patch(
            "importlib.metadata.version",
            side_effect=importlib.metadata.PackageNotFoundError,
        ):
            module = importlib.import_module("irsol_data_pipeline.version")

        assert module.__version__ == "0.0.0"
