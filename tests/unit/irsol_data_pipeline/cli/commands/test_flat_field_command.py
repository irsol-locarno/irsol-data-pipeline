"""Tests for the flat-field correction CLI commands."""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.core.models import (
    DayProcessingResult,
    FlatFieldCorrection,
)
from irsol_data_pipeline.pipeline.flatfield_cache import FlatFieldCache


def _make_ff_cache(*wavelengths: int) -> FlatFieldCache:
    """Build a FlatFieldCache pre-populated with stub corrections."""
    cache = FlatFieldCache()
    for wl in wavelengths:
        cache.add_correction(
            FlatFieldCorrection(
                source_flatfield_path=Path(f"/data/ff{wl}_m1.dat"),
                dust_flat=np.ones((4, 5)),
                offset_map=None,
                desmiled=np.ones((4, 5)),
                timestamp=datetime.datetime(
                    2024,
                    7,
                    13,
                    10,
                    0,
                    tzinfo=datetime.timezone.utc,
                ),
                wavelength=wl,
            ),
        )
    return cache


class TestFlatFieldApply:
    def test_apply_single_measurement_success(self, tmp_path: Path) -> None:
        """Apply processes a measurement and reports success."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        # A flat-field file in the same directory
        (tmp_path / "ff6302_m1.dat").write_text("placeholder")
        output_dir = tmp_path / "processed"

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
            ) as mock_process,
        ):
            result = app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_process.assert_called_once()
        call_kwargs = mock_process.call_args
        assert call_kwargs.kwargs["measurement_path"] == measurement.resolve()
        assert call_kwargs.kwargs["processed_dir"] == output_dir.resolve()

    def test_apply_passes_cache_dir_to_build_flatfield_cache(
        self,
        tmp_path: Path,
    ) -> None:
        """--cache-dir is forwarded to build_flatfield_cache."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        cache_dir = tmp_path / "my_cache"

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ) as mock_build,
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
            ),
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                    "--cache-dir",
                    str(cache_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_build.assert_called_once()
        assert mock_build.call_args.kwargs["cache_dir"] == cache_dir.resolve()

    def test_apply_no_flatfield_files_exits(self, tmp_path: Path) -> None:
        """Apply exits with code 1 when no flat-field files are found."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_existing_outputs_prompt_accept(self, tmp_path: Path) -> None:
        """Apply prompts when output files exist and proceeds on
        confirmation."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        output_dir.mkdir()
        # Simulate an existing corrected FITS
        (output_dir / "6302_m1_corrected.fits").write_text("placeholder")

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
                return_value=True,
            ) as mock_confirm,
        ):
            result = app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_called_once()
        assert result is None

    def test_apply_existing_outputs_prompt_decline_exits(self, tmp_path: Path) -> None:
        """Apply exits with code 1 when user declines the overwrite prompt."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        output_dir.mkdir()
        (output_dir / "6302_m1_corrected.fits").write_text("placeholder")

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
                return_value=False,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_force_skips_prompt_and_processed_check(self, tmp_path: Path) -> None:
        """--force skips the overwrite confirmation prompt."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        output_dir.mkdir()
        (output_dir / "6302_m1_corrected.fits").write_text("placeholder")

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
            ) as mock_confirm,
        ):
            result = app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                    "--force",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_not_called()
        assert result is None

    def test_apply_process_failure_exits(self, tmp_path: Path) -> None:
        """Apply exits with code 1 when process_single_measurement raises."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
                side_effect=RuntimeError("processing failed"),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_convert_on_ff_failure_converts_when_no_flatfields(
        self,
        tmp_path: Path,
    ) -> None:
        """With --convert-on-ff-failure and no flat-fields, converts instead of
        exiting."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.convert_measurement_to_fits",
            ) as mock_convert,
        ):
            result = app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                    "--convert-on-ff-failure",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_convert.assert_called_once()
        assert (
            mock_convert.call_args.kwargs["measurement_path"] == measurement.resolve()
        )
        assert mock_convert.call_args.kwargs["processed_dir"] == output_dir.resolve()

    def test_apply_convert_on_ff_failure_converts_after_process_failure(
        self,
        tmp_path: Path,
    ) -> None:
        """With --convert-on-ff-failure, a failed process triggers conversion
        (exits 1)."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        ff_cache = _make_ff_cache(6302)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[tmp_path / "ff6302_m1.dat"],
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.build_flatfield_cache",
                return_value=ff_cache,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_single_measurement",
                side_effect=RuntimeError("processing failed"),
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.convert_measurement_to_fits",
            ) as mock_convert,
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                    "--convert-on-ff-failure",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1
        mock_convert.assert_called_once()

    def test_apply_no_flatfield_without_convert_flag_exits(
        self, tmp_path: Path
    ) -> None:
        """Without --convert-on-ff-failure, no flat-fields still exits with
        code 1."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.discover_flatfield_files",
                return_value=[],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply",
                    str(measurement),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1


class TestFlatFieldApplyDay:
    def test_apply_day_success(self, tmp_path: Path) -> None:
        """Apply-day processes an observation day and prints a summary."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"

        day_result = DayProcessingResult(
            day_name="240713",
            processed=3,
            skipped=0,
            failed=0,
        )

        with patch(
            "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
            return_value=day_result,
        ) as mock_process:
            result = app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_process.assert_called_once()
        call_kwargs = mock_process.call_args.kwargs
        assert call_kwargs["day"].path == day_dir.resolve()
        assert call_kwargs["day"].processed_dir == output_dir.resolve()
        assert call_kwargs["force"] is False

    def test_apply_day_defaults_output_dir_to_day_processed(
        self,
        tmp_path: Path,
    ) -> None:
        """When --output-dir is omitted, processed_dir defaults to
        <day>/processed."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)

        day_result = DayProcessingResult(day_name="240713", processed=1)

        with patch(
            "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
            return_value=day_result,
        ) as mock_process:
            app(
                ["flat-field", "apply-day", str(day_dir)],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        call_day = mock_process.call_args.kwargs["day"]
        assert call_day.processed_dir == day_dir.resolve() / "processed"

    def test_apply_day_no_reduced_dir_exits(self, tmp_path: Path) -> None:
        """Apply-day exits with code 1 when the reduced/ directory is
        absent."""
        day_dir = tmp_path / "240713"
        day_dir.mkdir()

        with pytest.raises(SystemExit) as exc_info:
            app(
                ["flat-field", "apply-day", str(day_dir)],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_day_existing_output_dir_prompts(self, tmp_path: Path) -> None:
        """Apply-day prompts when --output-dir exists and has content."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "some_file.txt").write_text("existing")

        day_result = DayProcessingResult(day_name="240713", processed=1)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
                return_value=day_result,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
                return_value=True,
            ) as mock_confirm,
        ):
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_called_once()

    def test_apply_day_existing_output_dir_decline_exits(self, tmp_path: Path) -> None:
        """Apply-day exits with code 1 when user declines the confirmation."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "some_file.txt").write_text("existing")

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
                return_value=False,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_day_force_skips_prompt_and_passes_force_to_processor(
        self,
        tmp_path: Path,
    ) -> None:
        """--force skips the confirmation and passes force=True to the processor."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "some_file.txt").write_text("existing")

        day_result = DayProcessingResult(day_name="240713", processed=2)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
                return_value=day_result,
            ) as mock_process,
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.Confirm.ask",
            ) as mock_confirm,
        ):
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                    "--force",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_not_called()
        assert mock_process.call_args.kwargs["force"] is True

    def test_apply_day_failures_exit_with_code_1(self, tmp_path: Path) -> None:
        """Apply-day exits with code 1 when any measurement fails."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"

        day_result = DayProcessingResult(
            day_name="240713",
            processed=1,
            failed=2,
            errors=["6302_m2.dat: something went wrong", "6302_m3.dat: another error"],
        )

        with (
            patch(
                "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
                return_value=day_result,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_apply_day_convert_on_ff_failure_passed_to_processor(
        self,
        tmp_path: Path,
    ) -> None:
        """--convert-on-ff-failure is forwarded to process_observation_day."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"

        day_result = DayProcessingResult(day_name="240713", processed=1)

        with patch(
            "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
            return_value=day_result,
        ) as mock_process:
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                    "--convert-on-ff-failure",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert mock_process.call_args.kwargs["convert_on_ff_failure"] is True

    def test_apply_day_convert_on_ff_failure_defaults_false(
        self,
        tmp_path: Path,
    ) -> None:
        """convert_on_ff_failure defaults to False when flag is absent."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"

        day_result = DayProcessingResult(day_name="240713", processed=1)

        with patch(
            "irsol_data_pipeline.cli.commands.flat_field_command.process_observation_day",
            return_value=day_result,
        ) as mock_process:
            app(
                [
                    "flat-field",
                    "apply-day",
                    str(day_dir),
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert mock_process.call_args.kwargs["convert_on_ff_failure"] is False
