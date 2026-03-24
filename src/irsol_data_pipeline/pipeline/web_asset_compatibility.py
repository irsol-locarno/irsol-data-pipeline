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
    discover_day_web_asset_sources,
)
from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetFolderName,
    WebAssetSource,
)
from irsol_data_pipeline.exceptions import WebAssetUploadError


@dataclass(frozen=True)
class _DeploymentCandidate:
    """One asset tracked across planning, conversion, and upload phases."""

    source_png: Path
    staged_jpeg: Path
    target_path: str


def _build_deployment_candidates(
    sources: list[WebAssetSource],
    day_name: str,
) -> list[_DeploymentCandidate]:
    """Build deployment candidates with target paths for all discovered
    assets."""

    candidates: list[_DeploymentCandidate] = []
    for source in sources:
        target_path = str(
            PurePosixPath(WebAssetFolderName.for_asset_kind(source.kind).value)
            / day_name
            / f"{source.measurement_name}.jpg"
        )
        candidates.append(
            _DeploymentCandidate(
                source_png=source.source_path,
                staged_jpeg=source.target_path,
                target_path=target_path,
            )
        )
    return candidates


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

    Args:
        day: Observation day to process.
        remote_fs: :class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
        jpeg_quality: JPEG compression quality.
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
                staging_quicklook_root = (
                    staging_root / WebAssetFolderName.QUICK_LOOK.value
                )
                staging_context_root = staging_root / WebAssetFolderName.CONTEXT.value

                sources = discover_day_web_asset_sources(
                    day=day,
                    quicklook_root=staging_quicklook_root,
                    context_root=staging_context_root,
                )

                # 1) Gather all deployable assets with their ideal target path.
                planned_assets = _build_deployment_candidates(
                    sources=sources,
                    day_name=day.name,
                )

                assets_to_convert: list[_DeploymentCandidate] = []

                # 2) Check whether targets already exist and apply overwrite policy.
                for candidate in planned_assets:
                    with logger.contextualize(
                        source=candidate.source_png,
                        target=candidate.target_path,
                    ):
                        if (
                            remote_fs.file_exists(candidate.target_path)
                            and not force_overwrite
                        ):
                            skipped += 1
                            logger.info(
                                "Skipping existing web asset",
                            )
                            continue
                        assets_to_convert.append(candidate)

                assets_to_upload: list[_DeploymentCandidate] = []

                # 3) Convert remaining PNG sources into staged JPG files.
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
                        except Exception as exc:  # pragma: no cover - defensive catch
                            failed += 1
                            error_message = f"Failed conversion for {candidate.source_png.name}: {exc}"
                            errors.append(error_message)
                            logger.exception(
                                "Web asset conversion failed",
                            )

                # 4) Upload all converted files.
                ensured_remote_dirs = set()
                for candidate in assets_to_upload:
                    with logger.contextualize(
                        staged_source=candidate.staged_jpeg,
                        destination=candidate.target_path,
                    ):
                        try:
                            _upload_candidate(candidate, remote_fs, ensured_remote_dirs)
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
