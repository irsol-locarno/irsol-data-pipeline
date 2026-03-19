"""Centralized names and access helpers for Prefect Variables."""

from __future__ import annotations

from enum import Enum
from typing import Any

from loguru import logger
from prefect.variables import Variable


class PrefectVariableName(Enum):
    """Canonical Prefect Variable names used across flows and entrypoints."""

    JSOC_EMAIL = "jsoc-email"
    CACHE_EXPIRATION_HOURS = "cache-expiration-hours"
    FLOW_RUN_EXPIRATION_HOURS = "flow-run-expiration-hours"


def get_variable(name: PrefectVariableName, default: Any = None) -> Any:
    """Retrieve a Prefect Variable by name with logging.

    Args:
        name: The canonical variable name to look up.
        default: Value returned when the variable is not set in Prefect.

    Returns:
        The stored variable value, or ``default`` when not found.
    """
    not_found = object()
    with logger.contextualize(variable=name.value):
        value = Variable.get(name.value, default=not_found)
        if value is not_found:
            logger.warning(
                "Prefect Variable not set, using default",
                default=default,
            )
            value = default
        else:
            logger.debug("Resolved Prefect Variable", value=value)
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
        not_found = object()
        value = await Variable.aget(name.value, default=not_found)
        if value is not_found:
            logger.warning(
                "Prefect Variable not set, using default",
                default=default,
            )
            value = default
        else:
            logger.debug("Resolved Prefect Variable", value=value)
        return value
