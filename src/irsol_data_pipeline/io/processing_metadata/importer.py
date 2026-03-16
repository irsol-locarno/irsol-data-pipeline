import json
from pathlib import Path
from typing import Any

from loguru import logger


def read_metadata(path: Path) -> dict[str, Any]:
    """Read a metadata or error JSON file.

    Args:
        path: Path to the JSON file.

    Returns:
        Parsed dict.
    """
    with logger.contextualize(path=path):
        logger.debug("Reading metadata JSON")
        with path.open() as f:
            data = json.load(f)
        logger.debug("Metadata JSON loaded")
        return data
