"""Helpers for Prefect Secret access (PIOMBO_PASSWORD and others).

This module provides functions to retrieve secrets from Prefect Secret
blocks.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from loguru import logger
from prefect.blocks.system import Secret


class PrefectSecretName(Enum):
    """Canonical Prefect Secret block names used across flows and
    entrypoints."""

    PIOMBO_PASSWORD = "piombo-password"  # gitleaks:allow


def get_secret(name: PrefectSecretName, default: Any = None) -> Any:
    """Retrieve a secret value from a Prefect Secret block with logging.

    Args:
        name: The name of the Prefect Secret block.
        default: Value returned when the secret is not set in Prefect.

    Returns:
        The secret value, or ``default`` when not found.
    """
    with logger.contextualize(variable=name.value):
        try:
            value = Secret.load(name.value).get()
        except ValueError:
            logger.warning(
                "Prefect Secret not set, using default",
                default=default,
            )
            value = default
        else:
            logger.info("Resolved Prefect Secret")
        return value


async def aget_secret(name: PrefectSecretName, default: Any) -> Any:
    """Asynchronously retrieve a secret value from a Prefect Secret block with
    logging.

    Args:
        name: The name of the Prefect Secret block.
        default: Value returned when the secret is not set in Prefect.

    Returns:
        The secret value, or ``default`` when not found.
    """
    with logger.contextualize(secret_block=name):
        try:
            value = await Secret.aload(name.value)
        except ValueError:
            logger.warning(
                "Prefect Secret block not found, using default", default=default
            )
            value = default
        else:
            logger.info("Resolved Prefect Secret")
        return value
