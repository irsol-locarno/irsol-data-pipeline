"""Orchestration service for web-asset compatibility processing."""

from __future__ import annotations

import shutil
from collections import defaultdict
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
from irsol_data_pipeline.core.web_asset_compatibility.models import WebAssetKind
from irsol_data_pipeline.exceptions import WebAssetUploadError


def _remote_join(*parts: str) -> str:
    """Join path components using POSIX separators for remote paths."""

    return str(PurePosixPath(*parts))


def _destination_for(
    root: str | Path,
    day_name: str,
    measurement_name: str,
) -> Path:
    """Build the expected destination path for one converted asset."""

    return Path(root) / day_name / f"{measurement_name}.jpg"


def _upload_staged_day_assets(
    staged_day_dir: Path,
    destination_root: str | Path,
    day_name: str,
    force_overwrite: bool,
    remote_fs: RemoteFileSystem | None = None,
) -> None:
    """Upload staged JPG files for one day to a local or remote target.

    Args:
        staged_day_dir: Local day directory containing converted JPG files.
        destination_root: Local or remote destination root path.
        day_name: Observation day name used as destination subdirectory.
        force_overwrite: Overwrite already-existing destination files.
        remote_fs: Optional remote file-system adapter.  When provided, files
            are transferred to the remote host via the adapter; when ``None``,
            a plain local ``shutil.copy2`` is used instead.

    Raises:
        WebAssetUploadError: If transfer to local or remote destination fails.
    """

    if not staged_day_dir.is_dir():
        return

    if remote_fs is not None:
        remote_root = str(destination_root).strip()
        # Strip an optional rsync-like "user@host:/path" prefix
        if ":" in remote_root and not remote_root.startswith("/"):
            _, remote_root = remote_root.split(":", maxsplit=1)

        remote_day_dir = _remote_join(remote_root, day_name)
        remote_fs.ensure_dir(remote_day_dir)

        for staged_file in sorted(staged_day_dir.glob("*.jpg")):
            remote_file = _remote_join(remote_day_dir, staged_file.name)
            if not force_overwrite and remote_fs.file_exists(remote_file):
                continue
            remote_fs.upload_file(str(staged_file), remote_file)
        return

    destination_day_dir = Path(destination_root) / day_name
    destination_day_dir.mkdir(parents=True, exist_ok=True)
    for staged_file in sorted(staged_day_dir.glob("*.jpg")):
        target_file = destination_day_dir / staged_file.name
        if target_file.exists() and not force_overwrite:
            continue
        shutil.copy2(staged_file, target_file)


def process_day_web_asset_compatibility(
    day: ObservationDay,
    quicklook_root: str | Path,
    context_root: str | Path,
    remote_fs: RemoteFileSystem | None = None,
    jpeg_quality: int = 50,
    force_overwrite: bool = False,
    deploy_quicklook: bool = True,
    deploy_context: bool = True,
) -> DayProcessingResult:
    """Convert and deploy web assets for one day.

    Args:
        day: Observation day to process.
        quicklook_root: Root directory for quicklook output.
        context_root: Root directory for context output.
        remote_fs: :class:`~irsol_data_pipeline.core.remote_filesystem.RemoteFileSystem`
            or ``None``.  When provided, converted JPG files are transferred to
            the remote host via the adapter.  When ``None``, files are copied to
            a local destination directory.
        jpeg_quality: JPEG compression quality.
        force_overwrite: Overwrite existing JPG files when True.
        deploy_quicklook: Deploy corrected profile images when True.
        deploy_context: Deploy slit-preview images when True.

    Returns:
        DayProcessingResult with processed/skipped/failed counts.
    """
    with logger.contextualize(day=day.name):
        logger.info(
            "Starting web-assets compatibility processing",
            processed_dir=day.processed_dir,
            quicklook_root=quicklook_root,
            context_root=context_root,
            jpeg_quality=jpeg_quality,
            force_overwrite=force_overwrite,
            deploy_quicklook=deploy_quicklook,
            deploy_context=deploy_context,
        )

        _normalize_jpeg_quality(jpeg_quality)

        processed = 0
        skipped = 0
        failed = 0
        errors: list[str] = []
        staged_counts_by_kind: dict[WebAssetKind, int] = defaultdict(int)

        try:
            with TemporaryDirectory(prefix=f"web-assets-{day.name}-") as temp_dir:
                staging_root = Path(temp_dir)
                staging_quicklook_root = staging_root / "quicklook"
                staging_context_root = staging_root / "context"

                sources = discover_day_web_asset_sources(
                    day=day,
                    quicklook_root=staging_quicklook_root,
                    context_root=staging_context_root,
                    deploy_quicklook=deploy_quicklook,
                    deploy_context=deploy_context,
                )

                for source in sources:
                    destination_path = _destination_for(
                        root=(
                            quicklook_root
                            if source.kind is WebAssetKind.QUICK_LOOK
                            else context_root
                        ),
                        day_name=day.name,
                        measurement_name=source.measurement_name,
                    )

                    try:
                        if (
                            remote_fs is None
                            and destination_path.exists()
                            and not force_overwrite
                        ):
                            skipped += 1
                            logger.info(
                                "Skipping existing web asset",
                                kind=source.kind,
                                source=source.source_path,
                                target=destination_path,
                            )
                            continue

                        convert_png_to_jpeg(
                            source_path=source.source_path,
                            target_path=source.target_path,
                            jpeg_quality=jpeg_quality,
                        )
                        processed += 1
                        staged_counts_by_kind[source.kind] += 1
                        logger.info(
                            "Web asset converted to staging",
                            kind=source.kind,
                            source=source.source_path,
                            staged_target=source.target_path,
                            destination=destination_path,
                        )
                    except Exception as exc:  # pragma: no cover - defensive catch
                        failed += 1
                        error_message = (
                            f"Failed {source.kind} for {source.measurement_name}: {exc}"
                        )
                        errors.append(error_message)
                        logger.exception(
                            "Web asset conversion failed",
                            kind=source.kind,
                            source=source.source_path,
                            staged_target=source.target_path,
                            destination=destination_path,
                            error=error_message,
                        )

                if staged_counts_by_kind[WebAssetKind.QUICK_LOOK] > 0:
                    _upload_staged_day_assets(
                        staged_day_dir=staging_quicklook_root / day.name,
                        destination_root=quicklook_root,
                        day_name=day.name,
                        force_overwrite=force_overwrite,
                        remote_fs=remote_fs,
                    )

                if staged_counts_by_kind[WebAssetKind.CONTEXT] > 0:
                    _upload_staged_day_assets(
                        staged_day_dir=staging_context_root / day.name,
                        destination_root=context_root,
                        day_name=day.name,
                        force_overwrite=force_overwrite,
                        remote_fs=remote_fs,
                    )
        except (ValueError, WebAssetUploadError) as exc:
            failed += 1
            errors.append(str(exc))
            logger.exception("Web asset staging/upload failed", error=str(exc))

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
