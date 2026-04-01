"""Tests for flatfield_processor — focusing on the error-path behaviour."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from irsol_data_pipeline.core.models import (
    MeasurementMetadata,
    ObservationDay,
)
from irsol_data_pipeline.pipeline.flatfield_processor import process_observation_day


def _make_day(tmp_path: Path) -> ObservationDay:
    day_path = tmp_path / "250101"
    raw_dir = day_path / "raw"
    reduced_dir = day_path / "reduced"
    processed_dir = day_path / "processed"
    for directory in (raw_dir, reduced_dir, processed_dir):
        directory.mkdir(parents=True)
    return ObservationDay(
        path=day_path,
        raw_dir=raw_dir,
        reduced_dir=reduced_dir,
        processed_dir=processed_dir,
    )


def _write_measurement(reduced_dir: Path, name: str = "6302_m1.dat") -> Path:
    path = reduced_dir / name
    path.write_text("placeholder")
    return path


class TestProcessObservationDayErrorPath:
    """Tests for the error handling path in process_observation_day."""

    def test_plot_original_profile_called_on_error(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """When process_single_measurement raises, plot_original_profile is
        called regardless of convert_on_ff_failure."""
        day = _make_day(tmp_path)
        _write_measurement(day.reduced_dir)

        ff_cache = MagicMock()
        ff_cache.wavelengths = []

        with (
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_measurement_files",
                return_value=[day.reduced_dir / "6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_flatfield_files",
                return_value=[],
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.process_single_measurement",
                side_effect=RuntimeError("ff correction failed"),
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.processing_metadata_io"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.create_prefect_json_report"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.plot_original_profile",
            ) as mock_plot_original,
        ):
            result = process_observation_day(day, convert_on_ff_failure=False)

        assert result.failed == 1
        mock_plot_original.assert_called_once_with(
            measurement_path=day.reduced_dir / "6302_m1.dat",
            processed_dir=day.processed_dir,
        )

    def test_plot_original_profile_called_even_with_convert_on_ff_failure(
        self,
        tmp_path: Path,
        sample_measurement_metadata: MeasurementMetadata,
    ) -> None:
        """plot_original_profile is called regardless of convert_on_ff_failure
        when a measurement fails."""
        day = _make_day(tmp_path)
        _write_measurement(day.reduced_dir)

        ff_cache = MagicMock()
        ff_cache.wavelengths = []

        with (
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_measurement_files",
                return_value=[day.reduced_dir / "6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_flatfield_files",
                return_value=[],
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.process_single_measurement",
                side_effect=RuntimeError("ff correction failed"),
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.processing_metadata_io"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.create_prefect_json_report"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.plot_original_profile",
            ) as mock_plot_original,
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.convert_measurement_to_fits",
            ) as mock_convert,
        ):
            result = process_observation_day(day, convert_on_ff_failure=True)

        assert result.failed == 1
        mock_plot_original.assert_called_once()
        mock_convert.assert_called_once()

    def test_plot_original_profile_failure_does_not_stop_pipeline(
        self,
        tmp_path: Path,
    ) -> None:
        """If plot_original_profile itself raises, the pipeline continues and
        the result still reflects the original measurement failure."""
        day = _make_day(tmp_path)
        meas1 = _write_measurement(day.reduced_dir, "6302_m1.dat")
        meas2 = _write_measurement(day.reduced_dir, "6302_m2.dat")

        ff_cache = MagicMock()
        ff_cache.wavelengths = []

        with (
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_measurement_files",
                return_value=sorted([meas1, meas2]),
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.discover_flatfield_files",
                return_value=[],
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.process_single_measurement",
                side_effect=RuntimeError("ff correction failed"),
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.processing_metadata_io"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.create_prefect_json_report"
            ),
            patch(
                "irsol_data_pipeline.pipeline.flatfield_processor.plot_original_profile",
                side_effect=RuntimeError("plot failed"),
            ),
        ):
            result = process_observation_day(day, convert_on_ff_failure=False)

        # Both measurements fail their main processing; plot failures are
        # swallowed and the pipeline continues to the next measurement.
        assert result.failed == 2
        assert result.processed == 0
