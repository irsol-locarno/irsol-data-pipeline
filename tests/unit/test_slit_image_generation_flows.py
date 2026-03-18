"""Unit tests for slit image generation orchestration flows."""

from __future__ import annotations

from pathlib import Path

from irsol_data_pipeline.pipeline.filesystem import discover_observation_days


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

        observation_days = discover_observation_days(root)

        assert len(observation_days) == 2
        assert [day.name for day in observation_days] == ["20250312", "20250313"]
        assert observation_days[0].path == valid_day
        assert observation_days[0].reduced_dir == valid_day / "reduced"
