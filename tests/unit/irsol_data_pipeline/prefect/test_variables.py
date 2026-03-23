"""Tests for Prefect variable resolution helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from irsol_data_pipeline.exceptions import DatasetRootNotConfiguredError
from irsol_data_pipeline.prefect.variables import resolve_dataset_root


class TestResolveDatasetRoot:
    def test_uses_explicit_root_when_provided(self) -> None:
        result = resolve_dataset_root("/tmp/dataset")

        assert result == Path("/tmp/dataset")

    def test_uses_prefect_variable_when_root_missing(self) -> None:
        with patch(
            "irsol_data_pipeline.prefect.variables.get_variable",
            return_value="/srv/data",
        ):
            result = resolve_dataset_root("")

        assert result == Path("/srv/data")

    def test_raises_when_neither_root_nor_variable_are_available(self) -> None:
        with patch(
            "irsol_data_pipeline.prefect.variables.get_variable",
            return_value="",
        ):
            with pytest.raises(DatasetRootNotConfiguredError):
                resolve_dataset_root(None)
