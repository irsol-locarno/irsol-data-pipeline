"""Tests for web-asset compatibility pipeline orchestration."""

from __future__ import annotations

from pathlib import Path

from PIL import Image

from irsol_data_pipeline.core.config import (
    PROFILE_CORRECTED_PNG_SUFFIX,
    PROFILE_ORIGINAL_PNG_SUFFIX,
    SLIT_PREVIEW_PNG_SUFFIX,
)
from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.core.web_asset_compatibility.models import WebAssetFolderName
from irsol_data_pipeline.exceptions import WebAssetUploadError
from irsol_data_pipeline.pipeline.web_asset_compatibility import (
    process_day_web_asset_compatibility,
)


class TestRemoteFileSystem:
    """Tests for the SftpRemoteFileSystem adapter implementing the
    RemoteFileSystem protocol."""

    def __init__(
        self,
        existing_files: tuple[str, ...] = (),
        raise_on_upload: bool = False,
    ) -> None:
        self.existing_files = existing_files
        self.raise_on_upload = raise_on_upload

        self.created_dir: list[str] = []
        self.uploaded_files: list[tuple[str, str]] = []

    def ensure_dir(self, remote_dir: str):
        self.created_dir.append(remote_dir)

    def file_exists(self, remote_path: str) -> bool:
        if remote_path in self.existing_files:
            return True
        return remote_path in (r[1] for r in self.uploaded_files)

    def upload_file(self, local_path: str, remote_path: str):
        if self.raise_on_upload:
            raise WebAssetUploadError("permission denied")
        self.uploaded_files.append((local_path, remote_path))


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


class TestProcessDayWebAssetCompatibility:
    def test_upload_successful_single_context_file(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem()
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 1
        assert result.failed == 0
        assert result.skipped == 0
        assert remote_fs.created_dir == [
            str(Path(WebAssetFolderName.CONTEXT.value) / day.name),
        ]
        assert remote_fs.uploaded_files[0][1] == str(
            Path(WebAssetFolderName.CONTEXT.value) / day.name / "5876_m01.jpg",
        )

    def test_upload_successful_single_quick_look_file(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem()
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 1
        assert result.failed == 0
        assert result.skipped == 0
        assert remote_fs.created_dir == [
            str(Path(WebAssetFolderName.QUICK_LOOK.value) / day.name),
        ]
        assert remote_fs.uploaded_files[0][1] == str(
            Path(WebAssetFolderName.QUICK_LOOK.value) / day.name / "5876_m01.jpg",
        )

    def test_upload_successful_mixed_files(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")
        _write_png(day.processed_dir / f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem()
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 2
        assert result.failed == 0
        assert result.skipped == 0
        assert set(remote_fs.created_dir) == {
            str(Path(WebAssetFolderName.QUICK_LOOK.value) / day.name),
            str(Path(WebAssetFolderName.CONTEXT.value) / day.name),
        }
        assert set(r[1] for r in remote_fs.uploaded_files) == {
            str(Path(WebAssetFolderName.QUICK_LOOK.value) / day.name / "5876_m01.jpg"),
            str(Path(WebAssetFolderName.CONTEXT.value) / day.name / "5876_m01.jpg"),
        }

    def test_upload_failure_increments_failed(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem(raise_on_upload=True)

        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 1
        assert result.failed == 1
        assert any("permission denied" in error for error in result.errors)

    def test_skips_existing_remote_destination_without_overwrite(
        self,
        tmp_path: Path,
    ) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem(
            existing_files=(
                str(
                    Path(WebAssetFolderName.QUICK_LOOK.value)
                    / day.name
                    / "5876_m01.jpg",
                ),
            ),
        )

        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
            force_overwrite=False,
        )

        assert result.processed == 0
        assert result.skipped == 1
        assert result.failed == 0

    def test_uploads_and_skips_existing_remote_destination_with_overwrite(
        self,
        tmp_path: Path,
    ) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem(
            existing_files=(
                str(
                    Path(WebAssetFolderName.QUICK_LOOK.value)
                    / day.name
                    / "5876_m01.jpg",
                ),
            ),
        )

        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
            force_overwrite=True,
        )

        assert result.processed == 1
        assert result.skipped == 0
        assert result.failed == 0

    def test_uploads_some_and_skips_existing(self, tmp_path: Path) -> None:
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")
        _write_png(day.processed_dir / f"5876_m01{SLIT_PREVIEW_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem(
            existing_files=(
                str(
                    Path(WebAssetFolderName.QUICK_LOOK.value)
                    / day.name
                    / "5876_m01.jpg",
                ),
            ),
        )

        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
            force_overwrite=False,
        )

        assert result.processed == 1
        assert result.skipped == 1
        assert result.failed == 0

    def test_uploads_profile_original_when_corrected_absent(
        self,
        tmp_path: Path,
    ) -> None:
        """profile_original_png is uploaded as QUICK_LOOK when
        profile_corrected_png is absent (non-successful measurement)."""
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_ORIGINAL_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem()
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 1
        assert result.failed == 0
        assert remote_fs.uploaded_files[0][1] == str(
            Path(WebAssetFolderName.QUICK_LOOK.value) / day.name / "5876_m01.jpg",
        )

    def test_corrected_preferred_over_original_as_quicklook(
        self,
        tmp_path: Path,
    ) -> None:
        """When both profile_corrected and profile_original exist, only one
        QUICK_LOOK asset is uploaded (the corrected one)."""
        day = _make_day(tmp_path)
        _write_png(day.processed_dir / f"5876_m01{PROFILE_CORRECTED_PNG_SUFFIX}")
        _write_png(day.processed_dir / f"5876_m01{PROFILE_ORIGINAL_PNG_SUFFIX}")

        remote_fs = TestRemoteFileSystem()
        result = process_day_web_asset_compatibility(
            day=day,
            remote_fs=remote_fs,
        )

        assert result.processed == 1
        assert result.failed == 0
        quick_look_uploads = [
            r
            for r in remote_fs.uploaded_files
            if WebAssetFolderName.QUICK_LOOK.value in r[1]
        ]
        assert len(quick_look_uploads) == 1
