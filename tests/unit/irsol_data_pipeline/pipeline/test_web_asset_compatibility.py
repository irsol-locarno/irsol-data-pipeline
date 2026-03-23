"""Tests for web-asset compatibility pipeline orchestration."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from PIL import Image

from irsol_data_pipeline.core.config import PROFILE_CORRECTED_PNG_SUFFIX
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.core.remote_filesystem import RemoteFileSystem
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


def _make_mock_remote_fs() -> MagicMock:
    """Return a mock that satisfies the RemoteFileSystem protocol."""
    mock = MagicMock(spec=RemoteFileSystem)
    mock.file_exists.return_value = False
    return mock


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
            remote_fs=None,
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
            remote_fs=None,
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
            remote_fs=None,
        )

        assert destination_file.read_bytes() == b"new"

    def test_uses_remote_fs_when_provided(self, tmp_path: Path) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"data")

        remote_fs = _make_mock_remote_fs()
        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root="/var/www/assets",
            day_name="250101",
            force_overwrite=False,
            remote_fs=remote_fs,
        )

        remote_fs.ensure_dir.assert_called_once()
        remote_fs.upload_file.assert_called_once()

    def test_remote_fs_skips_existing_file_without_overwrite(
        self, tmp_path: Path
    ) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"data")

        remote_fs = _make_mock_remote_fs()
        remote_fs.file_exists.return_value = True

        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root="/var/www/assets",
            day_name="250101",
            force_overwrite=False,
            remote_fs=remote_fs,
        )

        remote_fs.upload_file.assert_not_called()

    def test_remote_fs_overwrites_existing_file_with_overwrite(
        self, tmp_path: Path
    ) -> None:
        staged_day = tmp_path / "staged" / "250101"
        staged_day.mkdir(parents=True)
        (staged_day / "5876_m01.jpg").write_bytes(b"data")

        remote_fs = _make_mock_remote_fs()
        remote_fs.file_exists.return_value = True

        _upload_staged_day_assets(
            staged_day_dir=staged_day,
            destination_root="/var/www/assets",
            day_name="250101",
            force_overwrite=True,
            remote_fs=remote_fs,
        )

        remote_fs.upload_file.assert_called_once()


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

        remote_fs = _make_mock_remote_fs()
        remote_fs.upload_file.side_effect = WebAssetUploadError("permission denied")

        result = process_day_web_asset_compatibility(
            day=day,
            quicklook_root="/irsol_db/docs/web-site/assets/img_quicklook",
            context_root="/irsol_db/docs/web-site/assets/img_data",
            remote_fs=remote_fs,
            deploy_context=False,
        )

        assert result.processed == 1
        assert result.failed == 1
        assert any("permission denied" in error for error in result.errors)

    def test_processes_with_remote_fs(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        remote_fs = _make_mock_remote_fs()
        result = process_day_web_asset_compatibility(
            day=day,
            quicklook_root="/irsol_db/docs/web-site/assets/img_quicklook",
            context_root="/irsol_db/docs/web-site/assets/img_data",
            remote_fs=remote_fs,
            deploy_context=False,
        )

        assert result.processed == 1
        assert result.failed == 0
        remote_fs.upload_file.assert_called_once()
