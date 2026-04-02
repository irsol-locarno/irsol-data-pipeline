"""Loguru logging configuration.

Configures one or two sinks:
- stdout with colorized, human-friendly formatting
- Optional rotating file sink for persistent logs (only when a log_file path is given)
"""

from __future__ import annotations

import sys
from typing import Literal

from loguru import logger

_configured = False


def _format_extra(record: dict) -> str:
    """Format extra kwargs as key=value pairs."""
    extra = {k: v for k, v in record["extra"].items() if not k.startswith("_")}
    if not extra:
        return ""
    pairs = " ".join(f"{k}={v}" for k, v in extra.items())
    return f" | {pairs}"


LOG_FORMAT_STDOUT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
    "{extra[_extra]}"
    "\n{exception}"
)

LOG_FORMAT_FILE = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | "
    "{level: <8} | "
    "{name}:{function}:{line} - "
    "{message}"
    "{extra[_extra]}"
    "\n{exception}"
)

LOG_LEVEL = Literal["TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def setup_logging(
    level: LOG_LEVEL = "INFO",
    log_file: str | None = None,
    rotation: str = "10 MB",
    retention: str = "1 week",
    force: bool = False,
) -> None:
    """Configure loguru sinks for the pipeline.

    Args:
        level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
        log_file: Path to the rotating log file. When ``None`` (the default)
            no file sink is created and log output goes to stdout only.
        rotation: When to rotate the log file (e.g. "10 MB", "1 day").
        retention: How long to keep rotated logs (e.g. "1 week", "30 days").
        force: Reconfigure logging even if setup was already called.
    """
    global _configured  # noqa PLW0603 - it's ok to handle globals in this case

    if _configured and not force:
        return

    # Reset the logger
    logger.remove()

    def _patch_extra(record: dict) -> None:
        """Inject pre-formatted extra string into the record."""
        record["extra"]["_extra"] = _format_extra(record)

    logger.configure(patcher=_patch_extra)

    # Stdout sink — colorized
    logger.add(
        sys.stdout,
        format=LOG_FORMAT_STDOUT,
        level=level,
        colorize=True,
    )

    # File sink — rotating (only when a path is provided)
    if log_file is not None:
        logger.add(
            log_file,
            format=LOG_FORMAT_FILE,
            level=level,
            rotation=rotation,
            retention=retention,
            encoding="utf-8",
        )

    _configured = True
