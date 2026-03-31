"""Tests for the slit-image generation CLI commands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from irsol_data_pipeline.cli import app
from irsol_data_pipeline.core.models import DayProcessingResult


class TestSlitImageGenerate:
    def test_generate_single_measurement_success(self, tmp_path: Path) -> None:
        """Generate processes a measurement and reports success."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.is_measurement_slit_preview_generated",
                return_value=False,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_image",
            ) as mock_generate,
        ):
            result = app(
                [
                    "slit-image",
                    "generate",
                    str(measurement),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_generate.assert_called_once()
        call_kwargs = mock_generate.call_args.kwargs
        assert call_kwargs["measurement_path"] == measurement.resolve()
        assert call_kwargs["processed_dir"] == output_dir.resolve()
        assert call_kwargs["jsoc_email"] == "test@example.com"

    def test_generate_passes_cache_dir(self, tmp_path: Path) -> None:
        """--cache-dir is forwarded to generate_slit_image as sdo_cache_dir."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        cache_dir = tmp_path / "sdo_cache"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.is_measurement_slit_preview_generated",
                return_value=False,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_image",
            ) as mock_generate,
        ):
            app(
                [
                    "slit-image",
                    "generate",
                    str(measurement),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                    "--cache-dir",
                    str(cache_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert mock_generate.call_args.kwargs["sdo_cache_dir"] == cache_dir.resolve()

    def test_generate_skips_when_artifact_exists(self, tmp_path: Path) -> None:
        """Generate skips without calling the processor when artifact
        exists."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"
        output_dir.mkdir()
        # Simulate an existing slit preview
        (output_dir / "6302_m1_slit_preview.png").write_text("placeholder")

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.is_measurement_slit_preview_generated",
                return_value=True,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_image",
            ) as mock_generate,
        ):
            result = app(
                [
                    "slit-image",
                    "generate",
                    str(measurement),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        # Should return cleanly (not exit with error) but not call the processor
        assert result is None
        mock_generate.assert_not_called()

    def test_generate_force_bypasses_skip_check(self, tmp_path: Path) -> None:
        """--force regenerates even when a slit preview artifact already exists."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.is_measurement_slit_preview_generated",
                return_value=True,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_image",
            ) as mock_generate,
        ):
            result = app(
                [
                    "slit-image",
                    "generate",
                    str(measurement),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                    "--force",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_generate.assert_called_once()

    def test_generate_process_failure_exits(self, tmp_path: Path) -> None:
        """Generate exits with code 1 when generate_slit_image raises."""
        measurement = tmp_path / "6302_m1.dat"
        measurement.write_text("placeholder")
        output_dir = tmp_path / "processed"

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.is_measurement_slit_preview_generated",
                return_value=False,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_image",
                side_effect=RuntimeError("slit image generation failed"),
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "slit-image",
                    "generate",
                    str(measurement),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1


class TestSlitImageGenerateDay:
    def test_generate_day_success(self, tmp_path: Path) -> None:
        """Generate-day processes an observation day and prints a summary."""
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
            "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_images_for_day",
            return_value=day_result,
        ) as mock_generate:
            result = app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert result is None
        mock_generate.assert_called_once()
        call_kwargs = mock_generate.call_args.kwargs
        assert call_kwargs["day"].path == day_dir.resolve()
        assert call_kwargs["day"].processed_dir == output_dir.resolve()
        assert call_kwargs["jsoc_email"] == "test@example.com"
        assert call_kwargs["force"] is False

    def test_generate_day_defaults_output_dir_to_day_processed(
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
            "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_images_for_day",
            return_value=day_result,
        ) as mock_generate:
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        call_day = mock_generate.call_args.kwargs["day"]
        assert call_day.processed_dir == day_dir.resolve() / "processed"

    def test_generate_day_no_reduced_dir_exits(self, tmp_path: Path) -> None:
        """Generate-day exits with code 1 when the reduced/ directory is
        absent."""
        day_dir = tmp_path / "240713"
        day_dir.mkdir()

        with pytest.raises(SystemExit) as exc_info:
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_generate_day_existing_output_dir_prompts(self, tmp_path: Path) -> None:
        """Generate-day prompts when output directory already has content."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "existing.txt").write_text("existing")

        day_result = DayProcessingResult(day_name="240713", processed=1)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_images_for_day",
                return_value=day_result,
            ),
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.Confirm.ask",
                return_value=True,
            ) as mock_confirm,
        ):
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_called_once()

    def test_generate_day_existing_output_dir_decline_exits(
        self,
        tmp_path: Path,
    ) -> None:
        """Generate-day exits with code 1 when user declines confirmation."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "existing.txt").write_text("existing")

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.Confirm.ask",
                return_value=False,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1

    def test_generate_day_force_skips_prompt_and_passes_force(
        self,
        tmp_path: Path,
    ) -> None:
        """--force skips confirmation and passes force=True to the processor."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        (output_dir / "existing.txt").write_text("existing")

        day_result = DayProcessingResult(day_name="240713", processed=2)

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_images_for_day",
                return_value=day_result,
            ) as mock_generate,
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.Confirm.ask",
            ) as mock_confirm,
        ):
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                    "--force",
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        mock_confirm.assert_not_called()
        assert mock_generate.call_args.kwargs["force"] is True

    def test_generate_day_failures_exit_with_code_1(self, tmp_path: Path) -> None:
        """Generate-day exits with code 1 when any measurement fails."""
        day_dir = tmp_path / "240713"
        reduced_dir = day_dir / "reduced"
        reduced_dir.mkdir(parents=True)
        output_dir = tmp_path / "output"

        day_result = DayProcessingResult(
            day_name="240713",
            processed=1,
            failed=1,
            errors=["6302_m2.dat: no SDO data"],
        )

        with (
            patch(
                "irsol_data_pipeline.cli.commands.slit_image_command.generate_slit_images_for_day",
                return_value=day_result,
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            app(
                [
                    "slit-image",
                    "generate-day",
                    str(day_dir),
                    "--jsoc-email",
                    "test@example.com",
                    "--output-dir",
                    str(output_dir),
                ],
                exit_on_error=False,
                print_error=False,
                result_action="return_value",
            )

        assert exc_info.value.code == 1
