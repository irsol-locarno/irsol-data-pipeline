"""Tests for web-asset compatibility pipeline orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from PIL import Image

from irsol_data_pipeline.core.config import PROFILE_CORRECTED_PNG_SUFFIX
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.exceptions import WebAssetUploadError
from irsol_data_pipeline.pipeline.web_asset_compatibility import (
    _upload_staged_day_assets,
    process_day_web_asset_compatibility,
)


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


def _write_png(path: Path) -> None:
    image = Image.new("RGB", (4, 4), color=(128, 64, 32))
    image.save(path, format="PNG")


class TestUploadStagedDayAssets:
    def test_uploads_to_local_destination(self, tmp_path: Path) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"a")

        destination_root = tmp_path / "dest"
        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root=destination_root,
            day_name="250101",
            force_overwrite=False,
            use_piombopload=False,
            piombo_hostname="",
            piombo_username="",
            piombo_password="",
        )

        assert (destination_root / "250101" / "5876_m01.jpg").exists()

    def test_skips_existing_local_file_without_overwrite(
        self,
        tmp_path: Path,
    ) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"new")

        destination_file = tmp_path / "dest" / "250101" / "5876_m01.jpg"
        destination_file.parent.mkdir(parents=True)
        destination_file.write_bytes(b"old")

        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root=tmp_path / "dest",
            day_name="250101",
            force_overwrite=False,
            use_piombo_upload=False,
            piombo_hostname="",
            piombo_username="",
            piombo_password="",
        )

        assert destination_file.read_bytes() == b"old"

    def test_overwrites_existing_local_file_with_overwrite(
        self,
        tmp_path: Path,
    ) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"new")

        destination_file = tmp_path / "dest" / "250101" / "5876_m01.jpg"
        destination_file.parent.mkdir(parents=True)
        destination_file.write_bytes(b"old")

        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root=tmp_path / "dest",
            day_name="250101",
            force_overwrite=True,
            use_piombo_upload=False,
            piombo_hostname="",
            piombo_username="",
            piombo_password="",
        )

        assert destination_file.read_bytes() == b"new"

    def test_uses_piombo_upload_when_enabled(self, tmp_path: Path) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"data")

        with patch(
            "irsol_data_pipeline.pipeline.web_asset_compatibility._upload_staged_day_assets_piombo"
        ) as mock_upload:
            _upload_staged_day_assets(
                staged_day_dir=staged_day,
                destination_root="/var/www/assets",
                day_name="250101",
                force_overwrite=False,
                use_piombo_upload=True,
                piombo_hostname="piombo7.usi.ch",
                piombo_username="user",
                piombo_password="secret",
            )

        assert mock_upload.call_count == 1


class TestProcessDayWebAssetCompatibility:
    def test_processes_and_uploads_to_local_destination(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        quicklook_root = tmp_path / "dest" / "quicklook"
        result = process_day_web_asset_compatibility(
            day=day,
            quicklook_root=quicklook_root,
            context_root=tmp_path / "dest" / "context",
            deploy_context=False,
        )

        assert result.processed == 1
        assert result.failed == 0
        assert result.skipped == 0
        assert (quicklook_root / day.name / "5876_m01.jpg").exists()

    def test_skips_existing_local_destination(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        quicklook_root = tmp_path / "dest" / "quicklook"
        existing_file = quicklook_root / day.name / "5876_m01.jpg"
        existing_file.parent.mkdir(parents=True)
        existing_file.write_bytes(b"already-here")

        result = process_day_web_asset_compatibility(
            day=day,
            quicklook_root=quicklook_root,
            context_root=tmp_path / "dest" / "context",
            deploy_context=False,
            force_overwrite=False,
        )

        assert result.processed == 0
        assert result.skipped == 1
        assert result.failed == 0

    def test_upload_failure_increments_failed(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        with patch(
            "irsol_data_pipeline.pipeline.web_asset_compatibility._upload_staged_day_assets_piombo",
            side_effect=WebAssetUploadError("permission denied"),
        ):
            result = process_day_web_asset_compatibility(
                day=day,
                quicklook_root="/irsol_db/docs/web-site/assets/img_quicklook",
                context_root="/irsol_db/docs/web-site/assets/img_data",
                piombo_hostname="piombo7.usi.ch",
                piombo_username="user",
                piombo_password="secret",
                deploy_context=False,
            )

        assert result.processed == 1
        assert result.failed == 1
        assert any("permission denied" in error for error in result.errors)

    def test_partial_piombo_credentials_fail_fast(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        result = process_day_web_asset_compatibility(
            day=day,
            quicklook_root=tmp_path / "dest" / "quicklook",
            context_root=tmp_path / "dest" / "context",
            piombo_hostname="piombo7.usi.ch",
            piombo_username="",
            piombo_password="",
            deploy_context=False,
        )

        assert result.processed == 0
        assert result.failed == 1
        assert any("must all be provided together" in error for error in result.errors)
