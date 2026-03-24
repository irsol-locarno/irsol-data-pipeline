"""Centralized names and access helpers for Prefect Variables."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any, cast

from loguru import logger
from prefect.variables import Variable

from irsol_data_pipeline.exceptions import DatasetRootNotConfiguredError


class PrefectVariableName(Enum):
    """Canonical Prefect Variable names used across flows and entrypoints."""

    DATA_ROOT_PATH = "data-root-path"
    JSOC_EMAIL = "jsoc-email"
    JSOC_DATA_DELAY_DAYS = "jsoc-data-delay-days"
    CACHE_EXPIRATION_HOURS = "cache-expiration-hours"
    FLOW_RUN_EXPIRATION_HOURS = "flow-run-expiration-hours"
    PIOMBO_BASE_PATH = "piombo-base-path"
    PIOMBO_HOSTNAME = "piombo-hostname"
    PIOMBO_USERNAME = "piombo-username"
    PIOMBO_PASSWORD = "piombo-password"


def get_variable(name: PrefectVariableName, default: Any = None) -> Any:
    """Retrieve a Prefect Variable by name with logging.

    Args:
        name: The canonical variable name to look up.
        default: Value returned when the variable is not set in Prefect.

    Returns:
        The stored variable value, or ``default`` when not found.
    """
    not_found = cast(Any, object())
    with logger.contextualize(variable=name.value):
        value = Variable.get(name.value, default=not_found)
        if value is not_found:
            logger.warning(
                "Prefect Variable not set, using default",
                default=default,
            )
            value = default
        else:
            logger.info("Resolved Prefect Variable", value=value)
        return value


async def aget_variable(name: PrefectVariableName, default: Any = None) -> Any:
    """Asynchronously retrieve a Prefect Variable by name with logging.

    Args:
        name: The canonical variable name to look up.
        default: Value returned when the variable is not set in Prefect.

    Returns:
        The stored variable value, or ``default`` when not found.
    """
    with logger.contextualize(variable=name.value):
        not_found = cast(Any, object())
        value = await Variable.aget(name.value, default=not_found)
        if value is not_found:
            logger.warning(
                "Prefect Variable not set, using default",
                default=default,
            )
            value = default
        else:
            logger.info("Resolved Prefect Variable", value=value)
        return value


def resolve_dataset_root(root: str | Path | None = None) -> Path:
    """Resolve the dataset root from an explicit argument or Prefect Variable.

    Args:
        root: Explicit dataset root path override.

    Returns:
        Resolved dataset root path.

    Raises:
        DatasetRootNotConfiguredError: If neither an argument nor a configured
            Prefect Variable is available.
    """

    if root is not None:
        explicit_root = str(root).strip()
        if explicit_root:
            logger.info("Using explicit dataset root argument", root=explicit_root)
            return Path(explicit_root)

    configured_root = str(
        get_variable(PrefectVariableName.DATA_ROOT_PATH, default="")
    ).strip()
    if configured_root:
        logger.info("Using dataset root from Prefect Variable", root=configured_root)
        return Path(configured_root)

    raise DatasetRootNotConfiguredError(PrefectVariableName.DATA_ROOT_PATH.value)
