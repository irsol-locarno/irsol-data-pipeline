"""Orchestration service for web-asset compatibility processing."""

from __future__ import annotations

import shutil
from collections import defaultdict
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any

import paramiko
from loguru import logger

from irsol_data_pipeline.core.models import DayProcessingResult, ObservationDay
from irsol_data_pipeline.core.web_asset_compatibility.conversion import (
    _normalize_jpeg_quality,
    convert_png_to_jpeg,
)
from irsol_data_pipeline.core.web_asset_compatibility.discovery import (
    discover_day_web_asset_sources,
)
from irsol_data_pipeline.core.web_asset_compatibility.models import WebAssetKind
from irsol_data_pipeline.exceptions import WebAssetUploadError


def _normalize_remote_root(destination_root: str | Path) -> str:
    """Return a remote POSIX root path from an operator-provided destination.

    Accepts either a direct path (e.g. ``/var/www/assets``) or an rsync-like
    destination (e.g. ``user@host:/var/www/assets``).
    """

    destination = str(destination_root).strip()
    if ":" in destination and not destination.startswith("/"):
        _, candidate_path = destination.split(":", maxsplit=1)
        return candidate_path
    return destination


def _remote_join(*parts: str) -> str:
    """Join path components using POSIX separators for SFTP paths."""

    return str(PurePosixPath(*parts))


def _ensure_remote_dir(sftp_client: Any, remote_dir: str) -> None:
    """Create a remote directory path recursively via SFTP.

    Args:
        sftp_client: Active Paramiko SFTP client.
        remote_dir: Absolute POSIX path on remote host.
    """

    if not remote_dir:
        return

    path_builder = PurePosixPath("/") if remote_dir.startswith("/") else PurePosixPath()
    for part in PurePosixPath(remote_dir).parts:
        if part == "/":
            continue
        path_builder = path_builder / part
        candidate = str(path_builder)
        try:
            sftp_client.stat(candidate)
        except OSError:
            sftp_client.mkdir(candidate)


def _upload_staged_day_assets_piombo(
    staged_day_dir: Path,
    destination_root: str | Path,
    day_name: str,
    force_overwrite: bool,
    hostname: str,
    username: str,
    password: str,
) -> None:
    """Upload staged JPG files for one day using Paramiko SFTP.

    Args:
        staged_day_dir: Local day directory containing staged JPG files.
        destination_root: Remote root directory path.
        day_name: Observation day subdirectory.
        force_overwrite: Overwrite existing remote files when True.
        hostname: SSH hostname.
        username: SSH username.
        password: SSH password.

    Raises:
        WebAssetUploadError: If Paramiko transport or upload fails.
    """

    if not staged_day_dir.is_dir():
        return

    remote_root = _normalize_remote_root(destination_root)
    remote_day_dir = _remote_join(remote_root, day_name)

    ssh_client = paramiko.SSHClient()
    sftp_client = None
    try:
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(
            hostname=hostname,
            username=username,
            password=password,
        )
        sftp_client = ssh_client.open_sftp()
        _ensure_remote_dir(sftp_client, remote_day_dir)

        for staged_file in sorted(staged_day_dir.glob("*.jpg")):
            remote_file = _remote_join(remote_day_dir, staged_file.name)
            if not force_overwrite:
                try:
                    sftp_client.stat(remote_file)
                    continue
                except OSError:
                    pass

            sftp_client.put(str(staged_file), remote_file)
    except Exception as exc:
        raise WebAssetUploadError(
            "Failed to upload staged web assets with paramiko"
            f" (host={hostname}, remote_dir={remote_day_dir})"
        ) from exc
    finally:
        if sftp_client is not None:
            sftp_client.close()
        ssh_client.close()


def _validate_piombo_credentials(
    piombo_hostname: str,
    piombo_username: str,
    piombo_password: str,
) -> bool:
    """Validate Piombo credential set and return whether upload is remote."""

    provided_fields = [
        bool(piombo_hostname.strip()),
        bool(piombo_username.strip()),
        bool(piombo_password.strip()),
    ]

    if any(provided_fields) and not all(provided_fields):
        raise ValueError(
            "piombo_hostname, piombosername, and piombo_password must "
            "all be provided together"
        )

    return all(provided_fields)


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
    use_piombo_upload: bool,
    piombo_hostname: str,
    piombo_username: str,
    piombo_password: str,
) -> None:
    """Upload staged JPG files for one day to a local or remote target.

    Args:
        staged_day_dir: Local day directory containing converted JPG files.
        destination_root: Local or remote destination root path.
        day_name: Observation day name used as destination subdirectory.
        force_overwrite: Overwrite already-existing destination files.
        use_piombo_upload: Whether to upload via Paramiko SFTP.
        piombo_hostname: SSH hostname.
        piombo_username: SSH username.
        piombo_password: SSH password.

    Raises:
        WebAssetUploadError: If transfer to local or remote destination fails.
    """

    if not staged_day_dir.is_dir():
        return

    if use_piombo_upload:
        _upload_staged_day_assets_piombo(
            staged_day_dir=staged_day_dir,
            destination_root=destination_root,
            day_name=day_name,
            force_overwrite=force_overwrite,
            hostname=piombo_hostname,
            username=piombo_username,
            password=piombo_password,
        )
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
    piombo_hostname: str = "",
    piombo_username: str = "",
    piombo_password: str = "",
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
        piomboostname: SSH hostname for remote upload.
        piombosername: SSH username for remote upload.
        piomboassword: SSH password for remote upload.
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
        use_piombo_upload = _validate_piombo_credentials(
            piombo_hostname=piombo_hostname,
            piombo_username=piombo_username,
            piombo_password=piombo_password,
        )

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
                            not use_piombo_upload
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
                        use_piombo_upload=use_piombo_upload,
                        piombo_hostname=piombo_hostname,
                        piombo_username=piombo_username,
                        piombo_password=piombo_password,
                    )

                if staged_counts_by_kind[WebAssetKind.CONTEXT] > 0:
                    _upload_staged_day_assets(
                        staged_day_dir=staging_context_root / day.name,
                        destination_root=context_root,
                        day_name=day.name,
                        force_overwrite=force_overwrite,
                        use_piombo_upload=use_piombo_upload,
                        piombo_hostname=piombo_hostname,
                        piombo_username=piombo_username,
                        piombo_password=piombo_password,
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
