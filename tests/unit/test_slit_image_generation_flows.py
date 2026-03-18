"""Unit tests for slit image generation orchestration flows."""

from __future__ import annotations

from pathlib import Path

from irsol_data_pipeline.orchestration.flows.slit_image_generation import (
    generate_daily_slit_images,
    generate_slit_images,
    scan_observation_days_task,
)


def _make_day(root: Path, year: str, day: str) -> Path:
    day_path = root / year / day
    (day_path / "reduced").mkdir(parents=True, exist_ok=True)
    return day_path


class TestScanObservationDaysTask:
    def test_discovers_only_days_with_reduced_dir(self, tmp_path: Path):
        root = tmp_path / "data"
        valid_day = _make_day(root, "2025", "20250312")
        _make_day(root, "2025", "20250313")
        # Invalid day: missing reduced dir, should be ignored.
        (root / "2025" / "20250314").mkdir(parents=True, exist_ok=True)

        observation_days = scan_observation_days_task.fn(root)

        assert len(observation_days) == 2
        assert [day.name for day in observation_days] == ["20250312", "20250313"]
        assert observation_days[0].path == valid_day
        assert observation_days[0].reduced_dir == valid_day / "reduced"


class TestGenerateSlitImagesFlow:
    def test_empty_dataset_returns_empty_results(self, tmp_path: Path):
        root = tmp_path / "data"
        root.mkdir(parents=True, exist_ok=True)

        results = generate_slit_images(
            root=str(root),
            jsoc_email="user@example.com",
            max_concurrent_days=1,
        )

        assert results == []

    def test_multiple_empty_days_return_zero_counts(self, tmp_path: Path):
        root = tmp_path / "data"
        _make_day(root, "2025", "20250312")
        _make_day(root, "2025", "20250313")

        results = generate_slit_images(
            root=str(root),
            jsoc_email="user@example.com",
            max_concurrent_days=1,
        )

        assert len(results) == 2
        assert sorted(result.day_name for result in results) == ["20250312", "20250313"]
        assert all(result.processed == 0 for result in results)
        assert all(result.skipped == 0 for result in results)
        assert all(result.failed == 0 for result in results)


class TestGenerateDailySlitImagesFlow:
    def test_empty_day_returns_zero_counts(self, tmp_path: Path):
        day_path = _make_day(tmp_path / "data", "2025", "20250312")

        result = generate_daily_slit_images(
            day_path=day_path,
            jsoc_email="user@example.com",
        )

        assert result.day_name == "20250312"
        assert result.processed == 0
        assert result.skipped == 0
        assert result.failed == 0
        assert result.errors == []
