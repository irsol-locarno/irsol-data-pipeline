"""Unit tests for slit image generation prefect flows."""

from __future__ import annotations

import datetime
from pathlib import Path

from irsol_data_pipeline.pipeline.filesystem import discover_observation_days
from irsol_data_pipeline.prefect.flows.slit_image_generation import (
    _build_min_age_day_predicate,
)


def _make_day(root: Path, year: str, day: str) -> Path:
    day_path = root / year / day
    (day_path / "reduced").mkdir(parents=True, exist_ok=True)
    return day_path


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
