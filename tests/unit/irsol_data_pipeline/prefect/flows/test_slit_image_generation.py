"""Unit tests for slit image generation prefect flows."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

from irsol_data_pipeline.core.models import ScanResult
from irsol_data_pipeline.pipeline.filesystem import discover_observation_days
from irsol_data_pipeline.prefect.flows.slit_image_generation import (
    _build_min_age_day_predicate,
    scan_slit_dataset_task,
)


def _make_day(root: Path, year: str, day: str) -> Path:
    day_path = root / year / day
    (day_path / "reduced").mkdir(parents=True, exist_ok=True)
    return day_path


class TestScanSlitDatasetTask:
    def test_returns_scan_result_with_pending_measurements(self, tmp_path: Path):
        root = tmp_path / "data"
        _make_day(root, "2025", "250312")

        with patch(
            "irsol_data_pipeline.prefect.flows.slit_image_generation.create_prefect_markdown_report"
        ):
            result = scan_slit_dataset_task(
                root=root,
                jsoc_data_delay_days=0,
            )

        assert isinstance(result, ScanResult)
        assert len(result.observation_days) == 1

    def test_days_with_all_previews_generated_are_not_pending(self, tmp_path: Path):
        root = tmp_path / "data"
        day_path = _make_day(root, "2025", "250312")
        reduced = day_path / "reduced"
        processed = day_path / "processed"
        processed.mkdir(parents=True, exist_ok=True)
        (reduced / "6302_m1.dat").touch()
        (processed / "6302_m1_slit_preview.png").touch()

        with patch(
            "irsol_data_pipeline.prefect.flows.slit_image_generation.create_prefect_markdown_report"
        ):
            result = scan_slit_dataset_task(
                root=root,
                jsoc_data_delay_days=0,
            )

        assert result.total_pending == 0
        assert "250312" not in result.pending_measurements

    def test_creates_prefect_markdown_report(self, tmp_path: Path):
        root = tmp_path / "data"
        _make_day(root, "2025", "250312")

        with patch(
            "irsol_data_pipeline.prefect.flows.slit_image_generation.create_prefect_markdown_report"
        ) as mock_report:
            scan_slit_dataset_task(root=root, jsoc_data_delay_days=0)

        mock_report.assert_called_once()


class TestScanObservationDaysTask:
    def test_discovers_only_days_with_reduced_dir(self, tmp_path: Path):
        root = tmp_path / "data"
        valid_day = _make_day(root, "2025", "250312")
        _make_day(root, "2025", "250313")
        # Invalid day: missing reduced dir, should be ignored.
        (root / "2025" / "250314").mkdir(parents=True, exist_ok=True)

        observation_days = discover_observation_days(root)

        assert len(observation_days) == 2
        assert [day.name for day in observation_days] == ["250312", "250313"]
        assert observation_days[0].path == valid_day
        assert observation_days[0].reduced_dir == valid_day / "reduced"


class TestDayPredicateHelpers:
    def test_min_age_predicate_is_inclusive(self, tmp_path: Path) -> None:
        root = tmp_path / "data"
        _make_day(root, "2026", "260309")
        _make_day(root, "2026", "260310")
        _make_day(root, "2026", "260311")

        predicate = _build_min_age_day_predicate(
            min_age_days=13,
            today=datetime.date(2026, 3, 23),
        )
        eligible_days = discover_observation_days(root, predicate=predicate)

        assert [day.name for day in eligible_days] == ["260309", "260310"]

    def test_min_age_predicate_excludes_invalid_day_names(self, tmp_path: Path) -> None:
        root = tmp_path / "data"
        _make_day(root, "2026", "260309")
        _make_day(root, "2026", "20260309")

        predicate = _build_min_age_day_predicate(
            min_age_days=1,
            today=datetime.date(2026, 3, 23),
        )
        eligible_days = discover_observation_days(root, predicate=predicate)

        assert [day.name for day in eligible_days] == ["260309"]
