"""Informational top-level CLI command."""

from __future__ import annotations

from typing import Any

from rich.table import Table

from irsol_data_pipeline.cli.common import (
    ensure_prefect_enabled,
    get_console,
    print_banner,
    print_json,
    safe_read_prefect_variable,
)
from irsol_data_pipeline.cli.metadata import (
    PREFECT_FLOW_GROUPS,
    PREFECT_VARIABLES,
    OutputFormat,
)
from irsol_data_pipeline.version import __version__


def _build_info_payload() -> dict[str, Any]:
    """Build the structured info payload.

    Returns:
        JSON-serializable runtime and metadata summary.
    """

    return {
        "flow_groups": [
            {
                "description": group.description,
                "flows": [flow.deployment_name for flow in group.flows],
                "name": group.name,
                "topic_tag": group.topic_tag.value,
            }
            for group in PREFECT_FLOW_GROUPS
        ],
        "prefect_variables": [
            {
                "name": variable.prefect_name.value,
                "status": safe_read_prefect_variable(variable.prefect_name.value)[1],
                "value": safe_read_prefect_variable(variable.prefect_name.value)[0],
            }
            for variable in PREFECT_VARIABLES
        ],
        "version": __version__,
    }


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

    variables_table = Table(
        title="Prefect Variables", show_header=True, header_style="bold cyan"
    )
    variables_table.add_column("Variable", style="white", no_wrap=True)
    variables_table.add_column("Status", style="magenta", no_wrap=True)
    variables_table.add_column("Value", style="white")
    for variable in payload["prefect_variables"]:
        variables_table.add_row(
            str(variable["name"]),
            str(variable["status"]),
            str(variable["value"] if variable["value"] is not None else "<unset>"),
        )
    get_console().print(variables_table)


def info(format: OutputFormat = "table", no_banner: bool = False) -> None:
    """Display runtime, flow-group, and variable status information.

    Args:
        format: Output format for the report.
        no_banner: Suppress the runtime banner.
    """

    ensure_prefect_enabled()
    print_banner(output_format=format, no_banner=no_banner)

    payload = _build_info_payload()
    if format == "json":
        print_json(payload)
        return

    _render_info_table(payload)
