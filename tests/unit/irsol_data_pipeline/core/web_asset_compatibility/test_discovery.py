"""Unit tests for the web_asset_compatibility package."""

from __future__ import annotations

from pathlib import Path

import pytest

from irsol_data_pipeline.core.config import (
    PROFILE_CORRECTED_PNG_SUFFIX,
    SLIT_PREVIEW_PNG_SUFFIX,
)
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.core.web_asset_compatibility.discovery import (
    _extract_measurement_name,
    discover_day_web_asset_sources,
)
from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetKind,
)


class TestExtractMeasurementName:
    @pytest.mark.parametrize(
        "filename,suffix,expected",
        [
            (
                f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}",
                PROFILE_CORRECTED_PNG_SUFFIX,
                "5876_m01",
            ),
            (
                f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}",
                SLIT_PREVIEW_PNG_SUFFIX,
                "5876_m01",
            ),
            (
                f"measurement_with_underscores{PROFILE_CORRECTED_PNG_SUFFIX}",
                PROFILE_CORRECTED_PNG_SUFFIX,
                "measurement_with_underscores",
            ),
        ],
    )
    def test_strips_known_suffix(
        self, filename: str, suffix: str, expected: str
    ) -> None:
        assert _extract_measurement_name(filename, suffix) == expected

    def test_raises_on_wrong_suffix(self) -> None:
        with pytest.raises(ValueError, match="expected suffix"):
            _extract_measurement_name("5876_m01.txt", PROFILE_CORRECTED_PNG_SUFFIX)

    def test_raises_on_partial_suffix(self) -> None:
        with pytest.raises(ValueError):
            _extract_measurement_name(
                "5876_m01_profile_corrected", PROFILE_CORRECTED_PNG_SUFFIX
            )


@pytest.fixture
def observation_day(tmp_path: Path) -> ObservationDay:
    day_path = tmp_path / "250101"
    raw_dir = day_path / "raw"
    reduced_dir = day_path / "reduced"
    processed_dir = day_path / "processed"
    for d in (raw_dir, reduced_dir, processed_dir):
        d.mkdir(parents=True)
    return ObservationDay(
        path=day_path,
        raw_dir=raw_dir,
        reduced_dir=reduced_dir,
        processed_dir=processed_dir,
    )


class TestDiscoverDayWebAssetSources:
    def test_returns_empty_when_processed_dir_missing(self, tmp_path: Path) -> None:
        day_path = tmp_path / "250101"
        day_path.mkdir()
        day = ObservationDay(
            path=day_path,
            raw_dir=day_path / "raw",
            reduced_dir=day_path / "reduced",
            processed_dir=day_path / "processed",  # does not exist
        )
        result = discover_day_web_asset_sources(
            day=day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )
        assert result == []

    def test_discovers_quicklook_sources(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        (
            observation_day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}"
        ).touch()
        (
            observation_day.processed_dir / f"5876_m02{PROFILE_CORRECTED_PNG_SUFFIX}"
        ).touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        assert len(result) == 2
        assert all(s.kind is WebAssetKind.QUICK_LOOK for s in result)
        measurement_names = [s.measurement_name for s in result]
        assert "5876_m01" in measurement_names
        assert "5876_m02" in measurement_names

    def test_discovers_context_sources(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        (observation_day.processed_dir / f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}").touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        assert len(result) == 1
        assert result[0].kind is WebAssetKind.CONTEXT
        assert result[0].measurement_name == "5876_m01"

    def test_discovers_both_kinds(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        (
            observation_day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}"
        ).touch()
        (observation_day.processed_dir / f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}").touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        assert len(result) == 2
        kinds = {s.kind for s in result}
        assert kinds == {WebAssetKind.QUICK_LOOK, WebAssetKind.CONTEXT}

    def test_result_is_sorted(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        for name in ["5876_m03", "5876_m01", "5876_m02"]:
            (
                observation_day.processed_dir / f"{name}{PROFILE_CORRECTED_PNG_SUFFIX}"
            ).touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        names = [s.measurement_name for s in result]
        assert names == sorted(names)

    def test_observation_name_matches_day_name(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        (
            observation_day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}"
        ).touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        assert result[0].observation_name == observation_day.name

    def test_ignores_non_png_files(
        self, tmp_path: Path, observation_day: ObservationDay
    ) -> None:
        (observation_day.processed_dir / "5876_m01.fits").touch()
        (observation_day.processed_dir / "5876_m01.dat").touch()

        result = discover_day_web_asset_sources(
            day=observation_day,
            quicklook_root=tmp_path / "ql",
            context_root=tmp_path / "ctx",
        )

        assert result == []
