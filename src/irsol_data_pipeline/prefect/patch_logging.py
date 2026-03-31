import logging as stdlib_logging
import traceback

from loguru import logger
from prefect.logging import get_run_logger

from irsol_data_pipeline.logging_config import LOG_LEVEL
from irsol_data_pipeline.logging_config import setup_logging as _setup_base_logging

_prefect_sink_added = False


def _extract_traceback_message(record: dict) -> str | None:
    """Extract a formatted traceback from a loguru record if present."""
    exception = record.get("exception")
    if exception is None:
        return None

    try:
        return "".join(
            traceback.format_exception(
                exception.type,
                exception.value,
                exception.traceback,
            ),
        ).strip()
    except Exception:
        return str(exception)


def setup_logging(level: LOG_LEVEL = "DEBUG"):
    """Configure loguru logging with a Prefect sink that forwards logs to the
    run logger."""
    global _prefect_sink_added  # noqa PLW0603 - it's ok to handle globals in this case
    if _prefect_sink_added:
        return

    _setup_base_logging(level=level)

    def _prefect_sink(message):
        record = message.record
        try:
            run_logger = get_run_logger()
        except Exception:
            return  # Not inside a flow/task run context

        # loguru and stdlib share numeric levels (DEBUG=10, INFO=20, etc.)
        # Map loguru-only levels: TRACE(5)->DEBUG(10), SUCCESS(25)->INFO(20)
        SUCCESS_LEVEL = 25
        level_no = record["level"].no
        if level_no < stdlib_logging.DEBUG:
            level_no = stdlib_logging.DEBUG
        elif level_no == SUCCESS_LEVEL:
            level_no = stdlib_logging.INFO

        traceback_message = _extract_traceback_message(record)
        extra_message = ", ".join(
            f"{k}={v}" for k, v in record["extra"].items() if k != "_extra"
        )
        full_message = (
            (f"{record['message']} | {extra_message}")
            if extra_message
            else record["message"]
        )
        if traceback_message:
            full_message = f"{full_message}\n{traceback_message}"

        run_logger.log(level_no, full_message)

    logger.add(_prefect_sink, format="{message}", level=level)
    _prefect_sink_added = True
