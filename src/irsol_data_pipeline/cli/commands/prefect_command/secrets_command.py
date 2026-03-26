"""Prefect secret CLI subcommands."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from cyclopts import App
from rich.table import Table

from irsol_data_pipeline.cli.common import (
    get_console,
    print_json,
)
from irsol_data_pipeline.cli.metadata import (
    PREFECT_SECRETS,
    OutputFormat,
    PrefectSecretMetadata,
)
from irsol_data_pipeline.prefect.secrets import get_secret

secrets_app = App(name="secrets", help="List and configure Prefect secrets.")


@dataclass(frozen=True)
class SecretReportEntry:
    """Operator-facing result for one Prefect secret.

    Attributes:
        name: Secret name.
        value: Current or selected value.
    """

    name: str
    value: str


def _get_secret_entries() -> list[SecretReportEntry]:
    entries: list[SecretReportEntry] = []
    for secret_meta in PREFECT_SECRETS:
        value = get_secret(secret_meta.prefect_name)
        entries.append(
            SecretReportEntry(
                name=secret_meta.prefect_name.value, value=value or "<unset>"
            )
        )
    return entries


def _render_secret_entries(entries: list[SecretReportEntry]) -> None:
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Secret", style="white", no_wrap=True)
    table.add_column("Value", style="white")
    for entry in entries:
        table.add_row(entry.name, entry.value if entry.value else "<unset>")
    get_console().print(table)


def _serialize_secret_entries(entries: list[SecretReportEntry]) -> dict[str, Any]:
    return {
        "secrets": [{"name": entry.name, "value": entry.value} for entry in entries]
    }


def _prompt_for_secret(config: PrefectSecretMetadata) -> str | None:
    value = input(
        f"{config.prompt_text}\nEnter value for secret '{config.prefect_name.value}': "
    ).strip()
    return value or None


def _confirm(prompt_text: str, *, default: bool = False) -> bool:
    response = input(prompt_text).strip().lower()
    if not response:
        return default
    return response in {"y", "yes"}


@secrets_app.command(name="list")
def list_secrets(format: OutputFormat = "table") -> None:
    """List current Prefect secret values."""
    entries = _get_secret_entries()
    if format == "json":
        print_json(_serialize_secret_entries(entries))
        return
    _render_secret_entries(entries)


@secrets_app.command(name="configure")
def configure_secrets(update_existing: bool = False) -> int:
    """Interactively configure Prefect secrets."""
    from prefect.blocks.system import Secret

    print("Prefect Secret Bootstrap\n")
    success_count = 0
    skipped_count = 0
    failed_count = 0
    already_set_count = 0
    report_entries: list[SecretReportEntry] = []
    for index, secret_meta in enumerate(PREFECT_SECRETS, start=1):
        total = len(PREFECT_SECRETS)
        remaining = total - index
        print(
            f"[{index}/{total}] {secret_meta.prefect_name.value} ({remaining} remaining)"
        )
        existing_value = get_secret(secret_meta.prefect_name, default=None)
        if existing_value:
            if not update_existing:
                print(f"  -> '{secret_meta.prefect_name.value}' already set")
                already_set_count += 1
                report_entries.append(
                    SecretReportEntry(
                        name=secret_meta.prefect_name.value, value=existing_value
                    )
                )
                continue
            if not _confirm(
                f"Secret '{secret_meta.prefect_name.value}' already set. Update it? [y/N]",
                default=False,
            ):
                print(
                    f"  -> Kept existing value for '{secret_meta.prefect_name.value}'"
                )
                already_set_count += 1
                report_entries.append(
                    SecretReportEntry(
                        name=secret_meta.prefect_name.value, value=existing_value
                    )
                )
                continue
        try:
            value = _prompt_for_secret(secret_meta)
            if value is None:
                print(f"  o Skipped '{secret_meta.prefect_name.value}'")
                skipped_count += 1
                report_entries.append(
                    SecretReportEntry(name=secret_meta.prefect_name.value, value="-")
                )
                continue
            if not _confirm(
                f"Set Prefect secret '{secret_meta.prefect_name.value}' to '[REDACTED]'? [y/N]",
                default=False,
            ):
                print(f"  o Skipped '{secret_meta.prefect_name.value}' (user declined)")
                skipped_count += 1
                report_entries.append(
                    SecretReportEntry(name=secret_meta.prefect_name.value, value="-")
                )
                continue
            Secret(value=value).save(secret_meta.prefect_name.value, overwrite=True)
            success_count += 1
            report_entries.append(
                SecretReportEntry(
                    name=secret_meta.prefect_name.value, value="[REDACTED]"
                )
            )
            print(
                f"  v {'Updated' if existing_value else 'Set'} '{secret_meta.prefect_name.value}'"
            )
        except Exception as exc:
            print(f"  x Failed to set '{secret_meta.prefect_name.value}': {exc}")
            failed_count += 1
            report_entries.append(
                SecretReportEntry(name=secret_meta.prefect_name.value, value="-")
            )
    print()
    print(
        f"Summary: {success_count} set or updated, {already_set_count} already set, {skipped_count} skipped, {failed_count} failed"
    )
    print()
    _render_secret_entries(report_entries)
    if success_count > 0 or already_set_count > 0:
        print("\nSecret bootstrap complete. You can now serve your deployments.")
    return 3 if failed_count > 0 else 0
