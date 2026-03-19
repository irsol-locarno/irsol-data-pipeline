"""Bootstrap Prefect variables from defaults or user input.

Run this once before serving any deployments:

    irsol-configure

It collects values interactively (optionally pre-filled with configured defaults)
and stores them as Prefect Variables for use in flows and deployments.

To add a new variable, add an entry to the VARIABLES registry.
"""

from __future__ import annotations

from typing import Any

import typer
from prefect.variables import Variable
from pydantic import BaseModel, Field
from rich.console import Console
from rich.table import Table

from irsol_data_pipeline.orchestration.flows.tags import DeploymentTopicTag
from irsol_data_pipeline.orchestration.variables import PrefectVariableName

app = typer.Typer()


class VariableConfig(BaseModel):
    """Configuration for a Prefect variable.

    Attributes:
        prefect_name: Name of the variable in Prefect.
        prompt_text: Text to display when prompting the user.
        default_value: Optional fallback value used as prompt default.
        required: Whether this variable is required.
        topic_tags: Deployment topic tags indicating where this variable is used.
    """

    prefect_name: PrefectVariableName
    prompt_text: str
    default_value: str | None = None
    required: bool = True
    topic_tags: list[DeploymentTopicTag] = Field(default_factory=list)


class VariableReportEntry(BaseModel):
    """Result information for one processed variable.

    Attributes:
        prefect_name: Name of the variable in Prefect.
        value: Value currently held (or selected) for the variable.
        tags: Topic tags associated with this variable.
        status: Processing result status.
    """

    prefect_name: PrefectVariableName
    value: Any
    tags: list[DeploymentTopicTag] = Field(default_factory=list)
    status: str


# Registry of variables to bootstrap.
# Add new variables here to expand the bootstrap script.
VARIABLES: list[VariableConfig] = [
    VariableConfig(
        prefect_name=PrefectVariableName.JSOC_EMAIL,
        prompt_text="JSOC email (register at http://jsoc.stanford.edu/ajax/register_email.html)",
        required=True,
        topic_tags=[DeploymentTopicTag.SLIT_IMAGES],
    ),
    VariableConfig(
        prefect_name=PrefectVariableName.CACHE_EXPIRATION_HOURS,
        prompt_text="Cache expiration time in hours (e.g. 4 weeks)",
        default_value=f"{24 * 7 * 4}",  # 4 weeks
        required=False,
        topic_tags=[DeploymentTopicTag.MAINTENANCE],
    ),
    VariableConfig(
        prefect_name=PrefectVariableName.FLOW_RUN_EXPIRATION_HOURS,
        prompt_text="Prefect flow-run history retention in hours (e.g. 4 weeks)",
        default_value=f"{24 * 7 * 4}",  # 4 weeks
        required=False,
        topic_tags=[DeploymentTopicTag.MAINTENANCE],
    ),
]


def _get_variable_value(config: VariableConfig) -> str | None:
    """Get variable value from default-backed user prompt.

    Args:
        config: Variable configuration.

    Returns:
        Variable value or None if not provided and not required.
    """
    if config.default_value is not None:
        typer.secho(
            (
                f"  Default value available for '{config.prefect_name.value}': "
                f"'{config.default_value}'"
            ),
            fg=typer.colors.CYAN,
        )
        value = typer.prompt(
            config.prompt_text,
            default=config.default_value,
            show_default=True,
        ).strip()
    else:
        value = typer.prompt(config.prompt_text).strip()

    if not value and config.required:
        typer.secho(
            f"  x {config.prefect_name.value} is required but empty",
            fg=typer.colors.RED,
        )
        return None

    return value or None


def _confirm_set_variable(prefect_name: PrefectVariableName, value: str) -> bool:
    """Confirm with user before setting a variable.

    Args:
        prefect_name: Name of the Prefect variable.
        value: Value to set.

    Returns:
        True if user confirmed, False otherwise.
    """
    return typer.confirm(f"Set Prefect variable '{prefect_name.value}' to '{value}'?")


def _handle_existing_variable(
    config: VariableConfig, existing_value: Any
) -> str | None:
    """Prompt the user to optionally update an existing Prefect variable.

    Args:
        config: Variable configuration.
        existing_value: Current value in Prefect.

    Returns:
        The new value to set, or None when the user chooses to keep the existing value.
    """
    existing_value_str = str(existing_value)
    if not typer.confirm(
        (
            f"Variable '{config.prefect_name.value}' already set to "
            f"'{existing_value_str}'. Update it?"
        ),
        default=False,
    ):
        return None

    if config.default_value is not None:
        if typer.confirm(
            (
                f"Use default value '{config.default_value}' for "
                f"'{config.prefect_name.value}'?"
            ),
            default=False,
        ):
            return config.default_value

    new_value = typer.prompt(
        f"New value for '{config.prefect_name.value}'",
        default=existing_value_str,
        show_default=True,
    ).strip()
    if not new_value and config.required:
        typer.secho(
            f"  x {config.prefect_name.value} is required but empty",
            fg=typer.colors.RED,
        )
        return None

    return new_value or None


def _format_topic_tags(tags: list[DeploymentTopicTag]) -> str:
    """Format topic tags for user-facing reports.

    Args:
        tags: Topic tags to display.

    Returns:
        Comma-separated tag string or '-' when empty.
    """
    if not tags:
        return "-"
    return ", ".join(tag.value for tag in tags)


def _render_report_table(entries: list[VariableReportEntry]) -> None:
    """Render a rich table for the final variable report.

    Args:
        entries: Processed variable report entries.
    """
    if not entries:
        return

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Variable", style="white", no_wrap=True)
    table.add_column("Value", style="white")
    table.add_column("Tags", style="magenta")
    table.add_column("Status", style="green")

    for entry in entries:
        table.add_row(
            entry.prefect_name.value,
            str(entry.value),
            _format_topic_tags(entry.tags),
            entry.status,
        )

    console = Console()
    console.print(table)


@app.command()
def main(
    update_existing: bool = typer.Option(
        False,
        "--update-existing",
        help="Prompt to update variables that already exist in Prefect.",
    ),
) -> None:
    """Bootstrap Prefect variables from defaults or user input.

    By default, variables already set in Prefect are skipped. Use
    --update-existing to prompt for updates on already-set variables.
    """
    typer.echo()
    typer.echo(
        typer.style("Prefect Variable Bootstrap", bold=True, fg=typer.colors.BLUE)
    )
    typer.echo()

    success_count = 0
    skipped_count = 0
    failed_count = 0
    already_set_count = 0
    report_entries: list[VariableReportEntry] = []

    for idx, config in enumerate(VARIABLES, start=1):
        total = len(VARIABLES)
        remaining = total - idx
        typer.echo(
            typer.style(
                f"[{idx}/{total}] {config.prefect_name.value} ({remaining} remaining)",
                fg=typer.colors.BRIGHT_BLACK,
            )
        )

        try:
            existing_value = Variable.get(config.prefect_name.value, default=None)
            if existing_value is not None:
                if not update_existing:
                    typer.secho(
                        f"  -> '{config.prefect_name.value}' already set",
                        fg=typer.colors.CYAN,
                    )
                    already_set_count += 1
                    report_entries.append(
                        VariableReportEntry(
                            prefect_name=config.prefect_name,
                            value=existing_value,
                            tags=config.topic_tags,
                            status="already-set",
                        )
                    )
                    continue

                new_value = _handle_existing_variable(config, existing_value)
                if new_value is None:
                    typer.secho(
                        f"  -> Kept existing value for '{config.prefect_name.value}'",
                        fg=typer.colors.CYAN,
                    )
                    already_set_count += 1
                    report_entries.append(
                        VariableReportEntry(
                            prefect_name=config.prefect_name,
                            value=existing_value,
                            tags=config.topic_tags,
                            status="kept-existing",
                        )
                    )
                    continue

                if not _confirm_set_variable(config.prefect_name, new_value):
                    typer.secho(
                        f"  o Skipped '{config.prefect_name.value}' (user declined)",
                        fg=typer.colors.YELLOW,
                    )
                    skipped_count += 1
                    report_entries.append(
                        VariableReportEntry(
                            prefect_name=config.prefect_name,
                            value=existing_value,
                            tags=config.topic_tags,
                            status="skipped-declined-update",
                        )
                    )
                    continue

                Variable.set(
                    config.prefect_name.value,
                    new_value,
                    overwrite=True,
                    tags=[tag.value for tag in config.topic_tags],
                )
                typer.secho(
                    f"  v Updated '{config.prefect_name.value}'",
                    fg=typer.colors.GREEN,
                )
                success_count += 1
                report_entries.append(
                    VariableReportEntry(
                        prefect_name=config.prefect_name,
                        value=new_value,
                        tags=config.topic_tags,
                        status="updated",
                    )
                )
                continue
        except Exception:
            existing_value = None

        try:
            value = _get_variable_value(config)
            if value is None:
                typer.secho(
                    f"  o Skipped '{config.prefect_name.value}'",
                    fg=typer.colors.YELLOW,
                )
                skipped_count += 1
                report_entries.append(
                    VariableReportEntry(
                        prefect_name=config.prefect_name,
                        value="-",
                        tags=config.topic_tags,
                        status="skipped-empty",
                    )
                )
                continue

            if not _confirm_set_variable(config.prefect_name, value):
                typer.secho(
                    f"  o Skipped '{config.prefect_name.value}' (user declined)",
                    fg=typer.colors.YELLOW,
                )
                skipped_count += 1
                report_entries.append(
                    VariableReportEntry(
                        prefect_name=config.prefect_name,
                        value="-",
                        tags=config.topic_tags,
                        status="skipped-declined-set",
                    )
                )
                continue

            Variable.set(
                config.prefect_name.value,
                value,
                overwrite=True,
                tags=[tag.value for tag in config.topic_tags],
            )
            typer.secho(
                f"  v Set '{config.prefect_name.value}'",
                fg=typer.colors.GREEN,
            )
            success_count += 1
            report_entries.append(
                VariableReportEntry(
                    prefect_name=config.prefect_name,
                    value=value,
                    tags=config.topic_tags,
                    status="set",
                )
            )

        except Exception as e:
            typer.secho(
                f"  x Failed to set '{config.prefect_name.value}': {e}",
                fg=typer.colors.RED,
            )
            failed_count += 1
            report_entries.append(
                VariableReportEntry(
                    prefect_name=config.prefect_name,
                    value="-",
                    tags=config.topic_tags,
                    status="failed",
                )
            )

    typer.echo()
    typer.echo(typer.style("-" * 50, fg=typer.colors.BLUE))
    typer.echo()

    summary_text = (
        "Summary: "
        f"{success_count} set or updated, "
        f"{already_set_count} already set, "
        f"{skipped_count} skipped, "
        f"{failed_count} failed"
    )
    summary_color = typer.colors.GREEN if failed_count == 0 else typer.colors.RED
    typer.secho(summary_text, bold=True, fg=summary_color)

    if report_entries:
        typer.echo()
        typer.echo(
            typer.style(
                "Variable Report:",
                bold=True,
                fg=typer.colors.BLUE,
            )
        )
        _render_report_table(report_entries)

    if success_count > 0 or already_set_count > 0:
        typer.echo()
        typer.secho(
            "Bootstrap complete. You can now serve your deployments.",
            fg=typer.colors.GREEN,
            bold=True,
        )

    typer.echo()

    if failed_count > 0:
        raise typer.Exit(code=1)
