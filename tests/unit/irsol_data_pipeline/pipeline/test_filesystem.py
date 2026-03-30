"""Tests for filesystem discovery utilities."""

from __future__ import annotations

from pathlib import Path

from irsol_data_pipeline.core.config import (
    CACHE_DIRNAME,
    CORRECTED_FITS_SUFFIX,
    ERROR_JSON_SUFFIX,
    FLATFIELD_CORRECTION_DATA_SUFFIX,
    METADATA_JSON_SUFFIX,
    PROCESSED_DIRNAME,
    PROFILE_CORRECTED_PNG_SUFFIX,
    PROFILE_ORIGINAL_PNG_SUFFIX,
    RAW_DIRNAME,
    REDUCED_DIRNAME,
)
from irsol_data_pipeline.pipeline.filesystem import (
    FLATFIELD_PATTERN,
    OBSERVATION_PATTERN,
    delete_empty_dirs,
    discover_flatfield_files,
    discover_measurement_files,
    discover_observation_days,
    flatfield_correction_cache_path,
    get_processed_stem,
    is_measurement_flat_field_processed,
    processed_cache_dir_for_day,
    processed_dir_for_day,
    processed_dir_for_measurement,
    processed_output_path,
    raw_dir_for_day,
    reduced_dir_for_day,
)


class TestPatterns:
    def test_observation_pattern_matches(self):
        assert OBSERVATION_PATTERN.match("6302_m1.dat")
        assert OBSERVATION_PATTERN.match("4078_m12.dat")
        assert OBSERVATION_PATTERN.match("5886_m1.dat")

    def test_observation_pattern_rejects(self):
        assert not OBSERVATION_PATTERN.match("ff6302_m1.dat")
        assert not OBSERVATION_PATTERN.match("cal6302_m1.dat")
        assert not OBSERVATION_PATTERN.match("dark6302_m1.dat")
        assert not OBSERVATION_PATTERN.match("6302_m1.sav")

    def test_flatfield_pattern_matches(self):
        assert FLATFIELD_PATTERN.match("ff6302_m1.dat")
        assert FLATFIELD_PATTERN.match("ff4078_m3.dat")

    def test_flatfield_pattern_rejects(self):
        assert not FLATFIELD_PATTERN.match("6302_m1.dat")
        assert not FLATFIELD_PATTERN.match("cal6302_m1.dat")


class TestDiscoverObservationDays:
    def test_discovers_days(self, tmp_path: Path):
        # Create: root/2024/240713/reduced/
        day_dir = tmp_path / "2024" / "240713"
        (day_dir / REDUCED_DIRNAME).mkdir(parents=True)
        (day_dir / RAW_DIRNAME).mkdir(parents=True)

        days = discover_observation_days(tmp_path)
        assert len(days) == 1
        assert days[0].name == "240713"
        assert days[0].raw_dir == day_dir / RAW_DIRNAME
        assert days[0].reduced_dir == day_dir / REDUCED_DIRNAME
        assert days[0].processed_dir == day_dir / PROCESSED_DIRNAME

    def test_skips_dirs_without_reduced(self, tmp_path: Path):
        day_dir = tmp_path / "2024" / "240713"
        day_dir.mkdir(parents=True)

        days = discover_observation_days(tmp_path)
        assert len(days) == 0

    def test_multiple_years(self, tmp_path: Path):
        for year, day in [("2024", "240713"), ("2025", "251111")]:
            d = tmp_path / year / day
            (d / REDUCED_DIRNAME).mkdir(parents=True)

        days = discover_observation_days(tmp_path)
        assert len(days) == 2

    def test_nonexistent_root(self, tmp_path: Path):
        days = discover_observation_days(tmp_path / "nonexistent")
        assert len(days) == 0

    def test_applies_predicate_filter(self, tmp_path):
        for year, day in [("2024", "240713"), ("2025", "251111")]:
            d = tmp_path / year / day
            (d / REDUCED_DIRNAME).mkdir(parents=True)

        days = discover_observation_days(
            tmp_path,
            predicate=lambda day: day.name.startswith("24"),
        )
        assert [day.name for day in days] == ["240713"]


class TestDiscoverMeasurementFiles:
    def test_finds_measurements(self, tmp_path: Path):
        (tmp_path / "6302_m1.dat").touch()
        (tmp_path / "6302_m2.dat").touch()
        (tmp_path / "ff6302_m1.dat").touch()
        (tmp_path / "cal6302_m1.dat").touch()
        (tmp_path / "dark2000_m1.dat").touch()

        files = discover_measurement_files(tmp_path)
        names = [f.name for f in files]
        assert "6302_m1.dat" in names
        assert "6302_m2.dat" in names
        assert "ff6302_m1.dat" not in names
        assert "cal6302_m1.dat" not in names
        assert "dark2000_m1.dat" not in names

    def test_empty_dir(self, tmp_path: Path):
        files = discover_measurement_files(tmp_path)
        assert len(files) == 0

    def test_nonexistent_dir(self, tmp_path: Path):
        files = discover_measurement_files(tmp_path / "nonexistent")
        assert len(files) == 0


class TestDiscoverFlatfieldFiles:
    def test_finds_flatfields(self, tmp_path: Path):
        (tmp_path / "ff6302_m1.dat").touch()
        (tmp_path / "ff4078_m1.dat").touch()
        (tmp_path / "6302_m1.dat").touch()

        files = discover_flatfield_files(tmp_path)
        names = [f.name for f in files]
        assert "ff6302_m1.dat" in names
        assert "ff4078_m1.dat" in names
        assert "6302_m1.dat" not in names


class TestGetProcessedStem:
    def test_basic(self):
        assert get_processed_stem("6302_m1.dat") == "6302_m1"

    def test_with_prefix(self):
        assert get_processed_stem("ff6302_m1.dat") == "ff6302_m1"


class TestDayDirectoryBuilders:
    def test_raw_dir_for_day(self, tmp_path: Path):
        day_path = tmp_path / "2025" / "251111"
        assert raw_dir_for_day(day_path) == day_path / RAW_DIRNAME

    def test_reduced_dir_for_day(self, tmp_path: Path):
        day_path = tmp_path / "2025" / "251111"
        assert reduced_dir_for_day(day_path) == day_path / REDUCED_DIRNAME

    def test_processed_dir_for_day(self, tmp_path: Path):
        day_path = tmp_path / "2025" / "251111"
        assert processed_dir_for_day(day_path) == day_path / PROCESSED_DIRNAME

    def test_processed_cache_dir_for_day(self, tmp_path: Path):
        day_path = tmp_path / "2025" / "251111"
        assert processed_cache_dir_for_day(day_path) == (
            day_path / PROCESSED_DIRNAME / CACHE_DIRNAME
        )


class TestMeasurementPathBuilders:
    def test_processed_dir_for_measurement(self, tmp_path: Path):
        measurement_path = (
            tmp_path / "2025" / "251111" / REDUCED_DIRNAME / "6302_m1.dat"
        )
        assert processed_dir_for_measurement(measurement_path) == (
            tmp_path / "2025" / "251111" / PROCESSED_DIRNAME
        )

    def test_flatfield_correction_cache_path(self, tmp_path: Path):
        flatfield_path = (
            tmp_path / "2025" / "251111" / REDUCED_DIRNAME / "ff6302_m3.dat"
        )
        assert flatfield_correction_cache_path(flatfield_path) == (
            tmp_path
            / "2025"
            / "251111"
            / PROCESSED_DIRNAME
            / CACHE_DIRNAME
            / "flat-field-cache"
            / "ff6302_m3_correction_cache.pkl"
        )


class TestProcessedOutputPath:
    def test_corrected_fits_path(self, tmp_path: Path):
        path = processed_output_path(tmp_path, "6302_m1.dat", kind="corrected_fits")
        assert path == tmp_path / f"6302_m1{CORRECTED_FITS_SUFFIX}"

    def test_error_json_path(self, tmp_path: Path):
        path = processed_output_path(tmp_path, "6302_m1.dat", kind="error_json")
        assert path == tmp_path / f"6302_m1{ERROR_JSON_SUFFIX}"

    def test_metadata_json_path(self, tmp_path: Path):
        path = processed_output_path(tmp_path, "6302_m1.dat", kind="metadata_json")
        assert path == tmp_path / f"6302_m1{METADATA_JSON_SUFFIX}"

    def test_flatfield_correction_data_path(self, tmp_path: Path):
        path = processed_output_path(
            tmp_path,
            "6302_m1.dat",
            kind="flatfield_correction_data",
        )
        assert path == tmp_path / f"6302_m1{FLATFIELD_CORRECTION_DATA_SUFFIX}"

    def test_profile_corrected_png_path(self, tmp_path: Path):
        path = processed_output_path(
            tmp_path,
            "6302_m1.dat",
            kind="profile_corrected_png",
        )
        assert path == tmp_path / f"6302_m1{PROFILE_CORRECTED_PNG_SUFFIX}"

    def test_profile_original_png_path(self, tmp_path: Path):
        path = processed_output_path(
            tmp_path,
            "6302_m1.dat",
            kind="profile_original_png",
        )
        assert path == tmp_path / f"6302_m1{PROFILE_ORIGINAL_PNG_SUFFIX}"

    def test_processed_output_uses_stem_of_source_name(self, tmp_path: Path):
        path = processed_output_path(tmp_path, "nested.name.dat", kind="corrected_fits")
        assert path == tmp_path / f"nested.name{CORRECTED_FITS_SUFFIX}"


class TestIsMeasurementProcessed:
    def test_not_processed(self, tmp_path: Path):
        assert not is_measurement_flat_field_processed(tmp_path, "6302_m1.dat")

    def test_corrected_fits_exists(self, tmp_path: Path):
        processed_output_path(tmp_path, "6302_m1.dat", kind="corrected_fits").touch()
        assert is_measurement_flat_field_processed(tmp_path, "6302_m1.dat")

    def test_error_exists(self, tmp_path: Path):
        processed_output_path(tmp_path, "6302_m1.dat", kind="error_json").touch()
        assert is_measurement_flat_field_processed(tmp_path, "6302_m1.dat")

    def test_prefers_centralized_output_builder(self, tmp_path: Path):
        corrected_path = processed_output_path(
            tmp_path,
            "4078_m12.dat",
            kind="corrected_fits",
        )
        corrected_path.touch()
        assert is_measurement_flat_field_processed(tmp_path, "4078_m12.dat")


class TestDeleteEmptyDirs:
    def test_delete_single_empty_dir(self, tmp_path: Path):
        empty_dir = tmp_path / "sub-dir"
        empty_dir.mkdir()
        assert empty_dir.exists()
        delete_empty_dirs(empty_dir)
        assert not empty_dir.exists()

    def test_delete_empty_dir_does_not_delete_dir_containing_file(self, tmp_path: Path):
        non_empty_dir = tmp_path / "sub-dir"
        non_empty_dir.mkdir()
        (non_empty_dir / "file").touch()
        delete_empty_dirs(non_empty_dir)
        assert non_empty_dir.exists()

    def test_delete_empty_dirs_recursively(self, tmp_path: Path):
        parent_dir = tmp_path / "sub-dir"
        child_dir = parent_dir / "sub-sub-dir"
        parent_dir.mkdir()
        child_dir.mkdir()
        assert child_dir.exists()
        delete_empty_dirs(parent_dir)
        assert not child_dir.exists()
        assert not parent_dir.exists()

    def test_delete_empty_dirs_recursively_but_not_those_containing_files(
        self, tmp_path: Path
    ):
        parent_dir = tmp_path / "sub-dir"
        empty_child_dir = parent_dir / "sub-sub-dir-empty"
        non_empty_child_dir = parent_dir / "sub-sub-dir-non-empty"
        parent_dir.mkdir()
        empty_child_dir.mkdir()
        non_empty_child_dir.mkdir()
        inner_file = non_empty_child_dir / "file"
        inner_file.touch()
        assert empty_child_dir.exists()
        assert non_empty_child_dir.exists()
        assert inner_file.exists()
        delete_empty_dirs(parent_dir)
        assert not empty_child_dir.exists()
        assert parent_dir.exists()
        assert non_empty_child_dir.exists()
        assert inner_file.exists()
