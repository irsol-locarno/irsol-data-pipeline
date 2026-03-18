import json
import string
from pathlib import Path
from typing import Callable

from irsol_data_pipeline.orchestration.decorators import prefect_enabled


def sanitize_artifact_title(title: str) -> str:
    """Sanitize a string to be used as a Prefect artifact title."""
    allowed_chars = string.ascii_lowercase + string.digits + "-"
    title = title.lower().replace("_", "-").replace("/", "-").replace(" ", "-")
    return "".join(c for c in title if c in allowed_chars)


def create_prefect_markdown_report(content: str, description: str):
    if prefect_enabled():
        from prefect.artifacts import create_markdown_artifact

        create_markdown_artifact(
            markdown=content,
            description=description,
        )


def create_prefect_progress_callback(name: str, total: int) -> Callable[[int], None]:
    if prefect_enabled():
        from prefect.artifacts import create_progress_artifact, update_progress_artifact

        progress_id = create_progress_artifact(
            0.0,
            key=sanitize_artifact_title(f"progress-{name}"),
            description=f"Processing progress for {name}",
        )

        def update_progress(processed: int):
            percent = (processed + 1) / total * 100
            update_progress_artifact(artifact_id=progress_id, progress=percent)
    else:

        def update_progress(processed: int):
            pass  # No-op if not using Prefect

    return update_progress


def _flatten_dict(d: dict, prefix: str = "") -> list[dict[str, str]]:
    """Recursively flatten a nested dict into a list of {"key", "value"} rows.

    Keys are joined with "." to reflect nesting depth, e.g. ``"a.b.c"``.
    """
    rows = []
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            rows.extend(_flatten_dict(v, prefix=full_key))
        else:
            rows.append({"key": full_key, "value": str(v)})
    return rows


def create_prefect_json_report(path: Path, title: str, key: str):
    if prefect_enabled():
        from prefect.artifacts import create_table_artifact

        with path.open() as f:
            content = json.load(f)

        create_table_artifact(
            table=_flatten_dict(content),
            key=sanitize_artifact_title(key),
            description=title,
        )


def create_image_artifact(path: Path, title: str, key: str):
    if prefect_enabled():
        from prefect.artifacts import create_image_artifact

        create_image_artifact(
            image_url=f"file://{path}",
            key=sanitize_artifact_title(key),
            description=title,
        )
