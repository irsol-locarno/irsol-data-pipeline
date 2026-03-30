"""Orchestration service for web-asset compatibility processing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory

from loguru import logger

from irsol_data_pipeline.core.models import DayProcessingResult, ObservationDay
from irsol_data_pipeline.core.remote_filesystem import RemoteFileSystem
from irsol_data_pipeline.core.web_asset_compatibility.conversion import (
    _normalize_jpeg_quality,
    convert_png_to_jpeg,
)
from irsol_data_pipeline.core.web_asset_compatibility.discovery import (
    discover_assets_for_measurement,
    discover_measurement_names,
)
from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetFolderName,
)
from irsol_data_pipeline.exceptions import WebAssetUploadError


@dataclass(frozen=True)
class _DeploymentCandidate:
    """One asset tracked across conversion and upload phases."""

    source_png: Path
    staged_jpeg: Path
    target_path: str


def _upload_candidate(
    candidate: _DeploymentCandidate,
    remote_fs: RemoteFileSystem,
    ensured_remote_dirs: set[str],
) -> None:
    """Upload one converted candidate to remote or local destination."""

    remote_dir = str(PurePosixPath(candidate.target_path).parent)
    if remote_dir not in ensured_remote_dirs:
        remote_fs.ensure_dir(remote_dir)
        ensured_remote_dirs.add(remote_dir)
    remote_fs.upload_file(str(candidate.staged_jpeg), candidate.target_path)


def process_day_web_asset_compatibility(
    day: ObservationDay,
    remote_fs: RemoteFileSystem,
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
) -> DayProcessingResult:
    """Convert and deploy web assets for one day.

    The pipeline proceeds in three phases:

    1. **Plan** — iterate over measurements in the day, identify context and
       quick-look assets for each, determine the remote target path, and
       check whether the remote file already exists.  Assets whose remote
       target already exists are skipped (unless *force_overwrite* is set).
    2. **Convert** — convert every pending asset PNG to JPEG inside a
       temporary staging directory.
    3. **Upload** — upload all successfully converted assets to the remote
       file system in a single batch.

    Args:
        day: Observation day to process.
        remote_fs: :class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
        jpeg_quality: JPEG compression quality (1–95).
        force_overwrite: Overwrite existing JPG files when True.

    Returns:
        DayProcessingResult with processed/skipped/failed counts.
    """
    with logger.contextualize(day=day.name):
        logger.info(
            "Starting web-assets compatibility processing",
            processed_dir=day.processed_dir,
            jpeg_quality=jpeg_quality,
            force_overwrite=force_overwrite,
        )

        _normalize_jpeg_quality(jpeg_quality)

        processed = 0
        skipped = 0
        failed = 0
        errors: list[str] = []

        try:
            with TemporaryDirectory(prefix=f"web-assets-{day.name}-") as temp_dir:
                staging_root = Path(temp_dir)

                # 1) For each measurement, identify assets and decide what
                #    needs converting based on remote existence.
                assets_to_convert: list[_DeploymentCandidate] = []

                measurement_names = discover_measurement_names(day.processed_dir)
                if not measurement_names:
                    logger.info("No measurements found")
                else:
                    logger.info(
                        "Found measurements to convert", count=len(measurement_names)
                    )
                    for measurement_name in measurement_names:
                        with logger.contextualize(measurement=measurement_name):
                            sources = discover_assets_for_measurement(
                                measurement_name=measurement_name,
                                observation_name=day.name,
                                processed_dir=day.processed_dir,
                            )
                            for source in sources:
                                target_path = source.remote_target_path
                                staged_jpeg = (
                                    staging_root
                                    / WebAssetFolderName.for_asset_kind(
                                        source.kind
                                    ).value
                                    / day.name
                                    / f"{measurement_name}.jpg"
                                )
                                with logger.contextualize(
                                    source=source.source_path,
                                    target=target_path,
                                ):
                                    if (
                                        remote_fs.file_exists(target_path)
                                        and not force_overwrite
                                    ):
                                        skipped += 1
                                        logger.info("Skipping existing web asset")
                                        continue
                                    assets_to_convert.append(
                                        _DeploymentCandidate(
                                            source_png=source.source_path,
                                            staged_jpeg=staged_jpeg,
                                            target_path=target_path,
                                        )
                                    )

                    if not assets_to_convert:
                        logger.info(
                            "All assets seem to be already deployed, nothing to convert/upload"
                        )
                    else:
                        logger.info(
                            "Planned web assets for conversion/upload",
                            count=len(assets_to_convert),
                        )
                        # 2) Convert all pending assets to JPEG in the staging area.
                        assets_to_upload: list[_DeploymentCandidate] = []
                        for candidate in assets_to_convert:
                            with logger.contextualize(
                                source=candidate.source_png,
                                staged_target=candidate.staged_jpeg,
                                destination=candidate.target_path,
                            ):
                                try:
                                    convert_png_to_jpeg(
                                        source_path=candidate.source_png,
                                        target_path=candidate.staged_jpeg,
                                        jpeg_quality=jpeg_quality,
                                    )
                                    processed += 1
                                    assets_to_upload.append(candidate)
                                    logger.info(
                                        "Web asset converted to staging",
                                    )
                                except (
                                    Exception
                                ) as exc:  # pragma: no cover - defensive catch
                                    failed += 1
                                    error_message = f"Failed conversion for {candidate.source_png.name}: {exc}"
                                    errors.append(error_message)
                                    logger.exception(
                                        "Web asset conversion failed",
                                    )

                        # 3) Upload all converted files.
                        ensured_remote_dirs: set[str] = set()
                        for candidate in assets_to_upload:
                            with logger.contextualize(
                                staged_source=candidate.staged_jpeg,
                                destination=candidate.target_path,
                            ):
                                try:
                                    _upload_candidate(
                                        candidate, remote_fs, ensured_remote_dirs
                                    )
                                except Exception as exc:
                                    failed += 1
                                    error_message = f"Failed upload for {candidate.staged_jpeg.name} to {candidate.target_path}: {exc}"
                                    errors.append(error_message)
                                    logger.exception(
                                        "Web asset upload failed",
                                    )
        except (ValueError, WebAssetUploadError) as exc:
            failed += 1
            errors.append(str(exc))
            logger.exception("Web asset staging/upload failed")

        logger.success(
            "Web-assets compatibility processing complete",
            processed=processed,
            skipped=skipped,
            failed=failed,
        )

        return DayProcessingResult(
            day_name=day.name,
            processed=processed,
            skipped=skipped,
            failed=failed,
            errors=errors,
        )
