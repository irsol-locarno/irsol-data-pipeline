import json
import string
from pathlib import Path

from irsol_data_pipeline.prefect.decorators import prefect_enabled


def sanitize_artifact_title(title: str) -> str:
    """Sanitize a string to be used as a Prefect artifact title."""
    allowed_chars = string.ascii_lowercase + string.digits + "-"
    title = title.lower().replace("_", "-").replace("/", "-").replace(" ", "-")
    return "".join(c for c in title if c in allowed_chars)


def create_prefect_markdown_report(content: str, description: str, key: str):
    if prefect_enabled():
        from prefect.artifacts import create_markdown_artifact

        create_markdown_artifact(
            markdown=content,
            description=description,
            key=sanitize_artifact_title(key),
        )


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
