"""Prefect variable CLI subcommands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cyclopts import App
from rich.table import Table

from irsol_data_pipeline.cli.common import (
    ensure_prefect_enabled,
    get_console,
    print_banner,
    print_json,
    safe_read_prefect_variable,
)
from irsol_data_pipeline.cli.metadata import (
    PREFECT_VARIABLES,
    OutputFormat,
    PrefectVariableMetadata,
)

variables_app = App(name="variables", help="List and configure Prefect variables.")


@dataclass(frozen=True)
class VariableReportEntry:
    """Operator-facing result for one Prefect variable.

    Attributes:
        name: Variable name.
        value: Current or selected value.
        required: Whether the variable is required.
        default_value: Configured default value.
        tags: Topic tags associated with the variable.
        status: Processing status string.
    """

    name: str
    value: Any
    required: bool
    default_value: str | None
    tags: tuple[str, ...]
    status: str


def _format_tags(tags: tuple[str, ...]) -> str:
    """Format tags for table output.

    Args:
        tags: Topic tags to format.

    Returns:
        Comma-separated tag string or `-` when empty.
    """

    if not tags:
        return "-"
    return ", ".join(tags)


def _read_current_value(variable_name: str) -> Any:
    """Read a current Prefect variable value.

    Args:
        variable_name: Prefect variable name.

    Returns:
        The stored variable value, or None when unset.
    """

    value, _ = safe_read_prefect_variable(variable_name)
    return value


def _get_variable_entries() -> list[VariableReportEntry]:
    """Collect the current variable report entries.

    Returns:
        Current variable report entries.
    """

    entries: list[VariableReportEntry] = []
    for variable in PREFECT_VARIABLES:
        current_value, status = safe_read_prefect_variable(variable.prefect_name.value)
        entries.append(
            VariableReportEntry(
                name=variable.prefect_name.value,
                value=current_value,
                required=variable.required,
                default_value=variable.default_value,
                tags=tuple(tag.value for tag in variable.topic_tags),
                status=status,
            )
        )
    return entries


def _render_variable_entries(entries: list[VariableReportEntry]) -> None:
    """Render variable entries as a Rich table.

    Args:
        entries: Variable report entries to display.
    """

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Variable", style="white", no_wrap=True)
    table.add_column("Value", style="white")
    table.add_column("Required", style="magenta", no_wrap=True)
    table.add_column("Default", style="white")
    table.add_column("Tags", style="green")
    table.add_column("Status", style="white", no_wrap=True)

    for entry in entries:
        table.add_row(
            entry.name,
            str(entry.value),
            "yes" if entry.required else "no",
            entry.default_value or "-",
            _format_tags(entry.tags),
            entry.status,
        )

    get_console().print(table)


def _serialize_variable_entries(entries: list[VariableReportEntry]) -> dict[str, Any]:
    """Convert variable entries to stable JSON output.

    Args:
        entries: Variable report entries.

    Returns:
        JSON-serializable variable payload.
    """

    return {
        "variables": [
            {
                "default_value": entry.default_value,
                "name": entry.name,
                "required": entry.required,
                "status": entry.status,
                "tags": list(entry.tags),
                "value": entry.value,
            }
            for entry in entries
        ]
    }


def _prompt_for_value(config: PrefectVariableMetadata) -> str | None:
    """Prompt the operator for a variable value.

    Args:
        config: Variable metadata.

    Returns:
        Selected value, or None when omitted for an optional variable.
    """

    if config.default_value is not None:
        raw_value = input(f"{config.prompt_text} [{config.default_value}]: ")
        value = raw_value.strip() or config.default_value
    else:
        value = input(f"{config.prompt_text}: ").strip()

    if not value and config.required:
        print(f"  x {config.prefect_name.value} is required but empty")
        return None

    return value or None


def _confirm(prompt_text: str, *, default: bool = False) -> bool:
    """Prompt for yes/no confirmation.

    Args:
        prompt_text: Text shown to the operator.
        default: Default confirmation choice.

    Returns:
        True when confirmed.
    """

    response = input(prompt_text).strip().lower()
    if not response:
        return default
    return response in {"y", "yes"}


@variables_app.command(name="list")
def list_variables(
    format: OutputFormat = "table",
    no_banner: bool = False,
) -> None:
    """List current Prefect variable values and metadata.

    Args:
        format: Output format for the report.
        no_banner: Suppress the runtime banner.
    """

    ensure_prefect_enabled()
    print_banner(output_format=format, no_banner=no_banner)

    entries = _get_variable_entries()
    if format == "json":
        print_json(_serialize_variable_entries(entries))
        return

    _render_variable_entries(entries)


@variables_app.command(name="configure")
def configure_variables(
    update_existing: bool = False,
    no_banner: bool = False,
) -> int:
    """Interactively configure Prefect variables.

    Args:
        update_existing: Prompt before updating variables that already exist.
        no_banner: Suppress the runtime banner.

    Returns:
        Exit code for the command.
    """

    ensure_prefect_enabled()
    print_banner(no_banner=no_banner)

    from prefect.variables import Variable

    print("Prefect Variable Bootstrap\n")

    success_count = 0
    skipped_count = 0
    failed_count = 0
    already_set_count = 0
    report_entries: list[VariableReportEntry] = []

    for index, config in enumerate(PREFECT_VARIABLES, start=1):
        total = len(PREFECT_VARIABLES)
        remaining = total - index
        print(f"[{index}/{total}] {config.prefect_name.value} ({remaining} remaining)")

        existing_value = Variable.get(config.prefect_name.value, default=None)
        if existing_value is not None:
            if not update_existing:
                print(f"  -> '{config.prefect_name.value}' already set")
                already_set_count += 1
                report_entries.append(
                    VariableReportEntry(
                        name=config.prefect_name.value,
                        value=existing_value,
                        required=config.required,
                        default_value=config.default_value,
                        tags=tuple(tag.value for tag in config.topic_tags),
                        status="already-set",
                    )
                )
                continue

            if not _confirm(
                (
                    f"Variable '{config.prefect_name.value}' already set to "
                    f"'{existing_value}'. Update it? [y/N]"
                ),
                default=False,
            ):
                print(f"  -> Kept existing value for '{config.prefect_name.value}'")
                already_set_count += 1
                report_entries.append(
                    VariableReportEntry(
                        name=config.prefect_name.value,
                        value=existing_value,
                        required=config.required,
                        default_value=config.default_value,
                        tags=tuple(tag.value for tag in config.topic_tags),
                        status="kept-existing",
                    )
                )
                continue

        try:
            value = _prompt_for_value(config)
            if value is None:
                print(f"  o Skipped '{config.prefect_name.value}'")
                skipped_count += 1
                report_entries.append(
                    VariableReportEntry(
                        name=config.prefect_name.value,
                        value="-",
                        required=config.required,
                        default_value=config.default_value,
                        tags=tuple(tag.value for tag in config.topic_tags),
                        status="skipped",
                    )
                )
                continue

            if not _confirm(
                f"Set Prefect variable '{config.prefect_name.value}' to '{value}'? [y/N]",
                default=False,
            ):
                print(f"  o Skipped '{config.prefect_name.value}' (user declined)")
                skipped_count += 1
                report_entries.append(
                    VariableReportEntry(
                        name=config.prefect_name.value,
                        value="-",
                        required=config.required,
                        default_value=config.default_value,
                        tags=tuple(tag.value for tag in config.topic_tags),
                        status="skipped",
                    )
                )
                continue

            Variable.set(
                config.prefect_name.value,
                value,
                overwrite=True,
                tags=[tag.value for tag in config.topic_tags],
            )
            success_count += 1
            report_entries.append(
                VariableReportEntry(
                    name=config.prefect_name.value,
                    value=value,
                    required=config.required,
                    default_value=config.default_value,
                    tags=tuple(tag.value for tag in config.topic_tags),
                    status="updated" if existing_value is not None else "set",
                )
            )
            print(
                f"  v {'Updated' if existing_value is not None else 'Set'} '{config.prefect_name.value}'"
            )
        except Exception as exc:
            print(f"  x Failed to set '{config.prefect_name.value}': {exc}")
            failed_count += 1
            report_entries.append(
                VariableReportEntry(
                    name=config.prefect_name.value,
                    value="-",
                    required=config.required,
                    default_value=config.default_value,
                    tags=tuple(tag.value for tag in config.topic_tags),
                    status="failed",
                )
            )

    print()
    print(
        (
            f"Summary: {success_count} set or updated, {already_set_count} already "
            f"set, {skipped_count} skipped, {failed_count} failed"
        )
    )
    print()
    _render_variable_entries(report_entries)
    if success_count > 0 or already_set_count > 0:
        print("\nBootstrap complete. You can now serve your deployments.")

    return 3 if failed_count > 0 else 0
