"""Informational top-level CLI command."""

from __future__ import annotations

import sys
import time
from contextlib import nullcontext
from typing import Any, Optional

import httpx
from rich.console import Console
from rich.status import Status
from rich.table import Table

from irsol_data_pipeline.cli.common import (
    get_console,
    print_json,
)
from irsol_data_pipeline.cli.metadata import (
    PREFECT_AUTOMATIONS,
    PREFECT_FLOW_GROUPS,
    PREFECT_SECRETS,
    PREFECT_VARIABLES,
    OutputFormat,
)
from irsol_data_pipeline.cli.presentation import (
    distribution_versions,
    print_runtime_presentation,
)
from irsol_data_pipeline.prefect.automations import get_automation
from irsol_data_pipeline.prefect.secrets import get_secret
from irsol_data_pipeline.prefect.variables import get_variable
from irsol_data_pipeline.version import __version__


def _build_flow_groups_payload() -> list[dict[str, Any]]:
    """Build the flow groups section of the info payload.

    Returns:
        List of flow group summaries.
    """

    return [
        {
            "name": group.name,
            "description": group.description,
            "flows": [flow.deployment_name for flow in group.flows],
            "topic_tag": group.topic_tag.value,
        }
        for group in PREFECT_FLOW_GROUPS
    ]


def _build_prefect_variables_payload() -> list[dict[str, Any]] | str:
    """Build the Prefect variables section of the info payload.

    Returns:
        List of Prefect variable summaries or an error message.
    """
    try:
        return [
            {
                "name": variable.prefect_name.value,
                "value": get_variable(variable.prefect_name),
            }
            for variable in PREFECT_VARIABLES
        ]
    except httpx.ConnectError:
        return "Error: Unable to connect to Prefect server to retrieve variable values."


def _build_prefect_secrets_payload() -> list[dict[str, str]]:
    """Build the Prefect secrets section of the info payload, masking
    values."""
    return [
        {
            "name": secret_meta.prefect_name.value,
            "value": "[REDACTED]"
            if get_secret(secret_meta.prefect_name)
            else "<unset>",
        }
        for secret_meta in PREFECT_SECRETS
    ]


def _build_prefect_automations_payload() -> list[dict[str, str]]:
    """Builds the Prefect automations section of the info payload."""
    remote_automations = [
        get_automation(automation.name) for automation in PREFECT_AUTOMATIONS
    ]
    remote_automations = [ra for ra in remote_automations if ra is not None]
    missing_remote_automations = [
        automation
        for automation in PREFECT_AUTOMATIONS
        if automation.name not in (ra.name for ra in remote_automations)
    ]

    result = []
    for remote_automation in remote_automations:
        result.append(
            {
                "name": remote_automation.name,
                "description": remote_automation.description,
                "deployed": True,
            }
        )
    for missing_automation in missing_remote_automations:
        result.append(
            {
                "name": missing_automation.name,
                "description": missing_automation.description,
                "deployed": False,
            }
        )
    return result


def _build_distributions_payload() -> list[dict[str, Any]]:
    """Build the distributions section of the info payload.

    Returns:
        List of distribution summaries.
    """

    return [
        {"name": name, "version": version}
        for name, version in distribution_versions().items()
    ]


def _build_info_payload(console: Optional[Console]) -> dict[str, Any]:
    """Build the structured info payload.

    Returns:
        JSON-serializable runtime and metadata summary.
    """

    context = (
        nullcontext() if console is None else console.status("Gathering information...")
    )

    def update(status: Optional[Status], message: str) -> None:
        if status is None:
            return
        status.update(status=message)

        time.sleep(0.2)

    try:
        result: dict[str, Any] = {}
        with context as status:
            result["version"] = __version__
            update(status, "Loading distributions")
            result["distributions"] = _build_distributions_payload()
            update(status, "Loading flow groups")
            result["flow_groups"] = _build_flow_groups_payload()
            update(status, "Loading prefect variables")
            result["prefect_variables"] = _build_prefect_variables_payload()
            update(status, "Loading prefect secrets")
            result["prefect_secrets"] = _build_prefect_secrets_payload()
            update(status, "Loading prefect automations")
            result["prefect_automations"] = _build_prefect_automations_payload()
    except httpx.NetworkError:
        if console is not None:
            console.print(
                "[bold red]Error connecting to Prefect server: are you sure prefect is running at the address configured via 'idp config user'?[/bold red]"
            )
        else:
            print_json(
                {
                    "error": "Error connecting to Prefect server: are you sure prefect is running at the address configured via 'idp config user'?"
                }
            )
        sys.exit(1)
    return result


def _render_info_table(payload: dict[str, Any]) -> None:
    """Render the human-readable info tables.

    Args:
        payload: Structured info payload.
    """

    runtime_table = Table(title="Runtime", show_header=True, header_style="bold cyan")
    runtime_table.add_column("Field", style="white", no_wrap=True)
    runtime_table.add_column("Value", style="white")
    runtime_table.add_row("Version", str(payload["version"]))
    get_console().print(runtime_table)

    distributions_table = Table(
        title="Distributions", show_header=True, header_style="bold cyan"
    )
    distributions_table.add_column("Name", style="white", no_wrap=True)
    distributions_table.add_column("Version", style="white")
    for distribution in payload["distributions"]:
        distributions_table.add_row(
            str(distribution["name"]),
            str(distribution["version"]),
        )
    get_console().print(distributions_table)

    flows_table = Table(title="Flow Groups", show_header=True, header_style="bold cyan")
    flows_table.add_column("Group", style="white", no_wrap=True)
    flows_table.add_column("Deployments", style="white")
    flows_table.add_column("Description", style="white")
    for group in payload["flow_groups"]:
        flows_table.add_row(
            str(group["name"]),
            ", ".join(group["flows"]),
            str(group["description"]),
        )
    get_console().print(flows_table)

    has_prefect_variables = isinstance(payload["prefect_variables"], list)
    if not has_prefect_variables:
        get_console().print(
            "[bold red]Error retrieving Prefect variables: make sure prefect is running via 'idp prefect start'[/bold red]"
        )
    else:
        variables_table = Table(
            title="Prefect Variables", show_header=True, header_style="bold cyan"
        )
        variables_table.add_column("Variable", style="white", no_wrap=True)
        variables_table.add_column("Value", style="white")
        for variable in payload["prefect_variables"]:
            variables_table.add_row(
                str(variable["name"]),
                str(variable["value"] if variable["value"] is not None else "<unset>"),
            )
        get_console().print(variables_table)

    has_prefect_secrets = isinstance(payload["prefect_secrets"], list)
    if not has_prefect_secrets:
        get_console().print(
            "[bold red]Error retrieving Prefect secrets: make sure prefect is running via 'idp prefect start'[/bold red]"
        )
    else:
        secrets_table = Table(
            title="Prefect Secrets", show_header=True, header_style="bold cyan"
        )
        secrets_table.add_column("Secret", style="white", no_wrap=True)
        secrets_table.add_column("Value", style="white")
        for secret in payload["prefect_secrets"]:
            secrets_table.add_row(
                str(secret["name"]),
                str(secret["value"]),
            )
        get_console().print(secrets_table)

    has_prefect_automations = isinstance(payload["prefect_automations"], list)
    if not has_prefect_automations:
        get_console().print(
            "[bold red]Error retrieving Prefect automations: make sure prefect is running via 'idp prefect start'[/bold red]"
        )
    else:
        automation_table = Table(
            title="Prefect Automations", show_header=True, header_style="bold cyan"
        )
        automation_table.add_column("Automation", style="white", no_wrap=True)
        automation_table.add_column("Description", style="white")
        automation_table.add_column("Deployed", style="white")
        for automation in payload["prefect_automations"]:
            deployed = automation["deployed"]
            color = "green" if deployed else "red"
            automation_table.add_row(
                f"[bold {color}]{automation['name']}[/bold {color}]",
                str(automation["description"]),
                f"[bold {color}]{deployed}[/bold {color}]",
            )
        get_console().print(automation_table)


def info(format: OutputFormat = "table") -> None:
    """Display runtime, flow-group, and variable status information.

    Args:
        format: Output format for the report.
    """

    if format == "json":
        console = None
    else:
        console = get_console()

    payload = _build_info_payload(console)

    if format == "json":
        print_json(payload)
        return

    print_runtime_presentation()
    _render_info_table(payload)
