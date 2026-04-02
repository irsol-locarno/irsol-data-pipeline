"""Interactive systemd service installer for the IRSOL data pipeline."""

from __future__ import annotations

import getpass
import shutil
import subprocess
from pathlib import Path
from string import Template

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from irsol_data_pipeline.cli.metadata import PREFECT_FLOW_GROUPS

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"

_SERVER_TEMPLATE_NAME = "irsol-prefect-server.service"
_FLOW_RUNNER_TEMPLATE_NAME = "irsol-prefect-flow-runner.service"

_SERVER_SERVICE_NAME = "irsol-prefect-server.service"

_DEFAULT_SYSTEMD_DIR = Path("/etc/systemd/system")
_DEFAULT_USER = "irsol-prefect"

_FLOW_GROUP_SERVICE_NAMES: dict[str, str] = {
    "flat-field-correction": "irsol-prefect-serve-flatfield.service",
    "slit-images": "irsol-prefect-serve-slitimages.service",
    "web-assets-compatibility": "irsol-prefect-serve-web-assets-compatibility.service",
    "maintenance": "irsol-prefect-serve-maintenance.service",
}

_FLOW_GROUP_DESCRIPTIONS: dict[str, str] = {
    "flat-field-correction": "Flat-Field Correction Runner",
    "slit-images": "Slit-Image Generation Runner",
    "web-assets-compatibility": "Web-Assets Compatibility Runner",
    "maintenance": "Maintenance Runner",
}


def _load_template(template_name: str) -> Template:
    """Load a service unit template from the templates directory.

    Args:
        template_name: Name of the template file.

    Returns:
        Parsed string template.
    """
    template_path = _TEMPLATES_DIR / template_name
    return Template(template_path.read_text())


def _detect_idp_path() -> str:
    """Detect the path to the ``idp`` executable.

    Returns:
        Absolute path to the ``idp`` binary, or the string ``"idp"``
        if it cannot be resolved.
    """
    idp_path = shutil.which("idp")
    if idp_path is not None:
        return str(Path(idp_path).resolve())
    return "idp"


def _is_service_registered(service_name: str) -> bool:
    """Check whether a systemd service unit is currently loaded.

    Args:
        service_name: Systemd unit name (e.g. ``irsol-prefect-server.service``).

    Returns:
        True when the unit file is recognized by systemd.
    """
    result = subprocess.run(  # noqa: S603
        ["systemctl", "is-enabled", service_name],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode == 0


def _service_file_exists(systemd_dir: Path, service_name: str) -> bool:
    """Check whether a service file already exists on disk.

    Args:
        systemd_dir: Directory where systemd unit files reside.
        service_name: Service file name.

    Returns:
        True when the file already exists.
    """
    return (systemd_dir / service_name).exists()


def _detect_existing_services(
    console: Console,
    systemd_dir: Path,
) -> dict[str, bool]:
    """Detect which pipeline services are already installed.

    Args:
        console: Rich console for output.
        systemd_dir: Directory where systemd unit files reside.

    Returns:
        Mapping of service name to whether the service is already installed.
    """
    all_services = [_SERVER_SERVICE_NAME, *_FLOW_GROUP_SERVICE_NAMES.values()]
    status: dict[str, bool] = {}
    for service_name in all_services:
        exists = _service_file_exists(systemd_dir, service_name)
        registered = _is_service_registered(service_name)
        status[service_name] = exists or registered
    return status


def _render_existing_services(
    console: Console,
    status: dict[str, bool],
) -> None:
    """Render a table showing which services are already installed.

    Args:
        console: Rich console for output.
        status: Mapping of service name to installation status.
    """
    any_installed = any(status.values())
    if not any_installed:
        console.print(
            "[dim]No existing pipeline services detected.[/dim]\n",
        )
        return

    table = Table(title="Existing Services", show_header=True, header_style="bold cyan")
    table.add_column("Service", style="white", no_wrap=True)
    table.add_column("Status", style="white", no_wrap=True)
    for service_name, installed in status.items():
        marker = "[green]installed[/green]" if installed else "[dim]not installed[/dim]"
        table.add_row(service_name, marker)
    console.print(table)
    console.print()


def _prompt_unix_user(console: Console) -> str:
    """Prompt for the Unix user that will run the services.

    Args:
        console: Rich console for output.

    Returns:
        Selected Unix username.
    """
    current_user = getpass.getuser()
    default = _DEFAULT_USER if current_user == "root" else current_user
    return Prompt.ask(
        "Unix user to run the services",
        default=default,
        console=console,
    )


def _prompt_idp_path(console: Console) -> str:
    """Prompt for the path to the ``idp`` executable.

    Args:
        console: Rich console for output.

    Returns:
        Absolute path string to the ``idp`` binary.
    """
    detected = _detect_idp_path()
    return Prompt.ask(
        "Path to the [bold]idp[/bold] executable",
        default=detected,
        console=console,
    )


def _prompt_systemd_dir(console: Console) -> Path:
    """Prompt for the target systemd unit directory.

    Args:
        console: Rich console for output.

    Returns:
        Target directory as a Path.
    """
    raw = Prompt.ask(
        "Systemd unit directory",
        default=str(_DEFAULT_SYSTEMD_DIR),
        console=console,
    )
    return Path(raw).expanduser().resolve()


def _prompt_flow_groups(console: Console) -> list[str]:
    """Prompt the user to select which flow runner services to install.

    Args:
        console: Rich console for output.

    Returns:
        List of selected flow-group names.
    """
    selected: list[str] = []
    for group in PREFECT_FLOW_GROUPS:
        if Confirm.ask(
            f"Install service for [bold]{group.name}[/bold] ({group.description})?",
            default=True,
            console=console,
        ):
            selected.append(group.name)
    return selected


def _generate_server_unit(user: str, idp_path: str) -> str:
    """Render the Prefect server systemd unit file.

    Args:
        user: Unix user for the service.
        idp_path: Absolute path to the ``idp`` executable.

    Returns:
        Rendered unit file content.
    """
    template = _load_template(_SERVER_TEMPLATE_NAME)
    return template.substitute(user=user, idp_executable_path=idp_path)


def _generate_flow_runner_unit(
    user: str,
    idp_path: str,
    flow_group_name: str,
) -> str:
    """Render a flow-runner systemd unit file.

    Args:
        user: Unix user for the service.
        idp_path: Absolute path to the ``idp`` executable.
        flow_group_name: Canonical flow-group name.

    Returns:
        Rendered unit file content.
    """
    template = _load_template(_FLOW_RUNNER_TEMPLATE_NAME)
    description = _FLOW_GROUP_DESCRIPTIONS[flow_group_name]
    return template.substitute(
        user=user,
        idp_executable_path=idp_path,
        flow_group_name=flow_group_name,
        flow_group_description=description,
    )


def _write_unit_file(
    console: Console,
    systemd_dir: Path,
    service_name: str,
    content: str,
    *,
    overwrite: bool,
) -> bool:
    """Write a systemd unit file to the target directory.

    Args:
        console: Rich console for output.
        systemd_dir: Target directory for the unit file.
        service_name: Service file name.
        content: Rendered unit file content.
        overwrite: Whether to overwrite an existing file without prompting.

    Returns:
        True when the file was written.
    """
    target = systemd_dir / service_name
    if (
        target.exists()
        and not overwrite
        and not Confirm.ask(
            f"[yellow]{target}[/yellow] already exists. Overwrite?",
            default=False,
            console=console,
        )
    ):
        console.print(f"  [dim]Skipped {service_name}[/dim]")
        return False

    target.write_text(content)
    console.print(f"  [green]✓[/green] Written [bold]{target}[/bold]")
    return True


def _render_post_install_instructions(
    console: Console,
    written_services: list[str],
) -> None:
    """Render post-installation instructions.

    Args:
        console: Rich console for output.
        written_services: List of service file names that were written.
    """
    if not written_services:
        return

    lines = ["sudo systemctl daemon-reload"]
    lines.extend(f"sudo systemctl enable --now {svc}" for svc in written_services)

    instructions = "\n".join(lines)
    console.print()
    console.print(
        Panel(
            instructions,
            title="Next steps",
            subtitle="Run these commands to activate the services",
            border_style="green",
        ),
    )


def install_service() -> int:
    """Interactively generate and install systemd service unit files.

    Walks the user through selecting a Unix user, the ``idp`` executable
    path, the target systemd directory, and which flow-runner services to
    install.  Detects services that are already registered and warns before
    overwriting.

    Returns:
        Exit code for the command.
    """
    console = Console()

    console.print()
    console.print(
        Panel(
            "[bold]IRSOL Data Pipeline — Service Installer[/bold]\n\n"
            "This command generates systemd service unit files for the\n"
            "Prefect server and flow runners, and writes them to the\n"
            "target directory.",
            border_style="blue",
        ),
    )
    console.print()

    systemd_dir = _prompt_systemd_dir(console)

    existing = _detect_existing_services(console, systemd_dir)
    _render_existing_services(console, existing)

    user = _prompt_unix_user(console)
    idp_path = _prompt_idp_path(console)

    install_server = Confirm.ask(
        "Install the [bold]Prefect server[/bold] service?",
        default=True,
        console=console,
    )
    selected_groups = _prompt_flow_groups(console)

    if not install_server and not selected_groups:
        console.print("\n[yellow]No services selected. Nothing to do.[/yellow]")
        return 0

    console.print()
    systemd_dir.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    overwrite = False

    if install_server:
        content = _generate_server_unit(user, idp_path)
        if _write_unit_file(
            console,
            systemd_dir,
            _SERVER_SERVICE_NAME,
            content,
            overwrite=overwrite,
        ):
            written.append(_SERVER_SERVICE_NAME)

    for group_name in selected_groups:
        service_name = _FLOW_GROUP_SERVICE_NAMES[group_name]
        content = _generate_flow_runner_unit(user, idp_path, group_name)
        if _write_unit_file(
            console,
            systemd_dir,
            service_name,
            content,
            overwrite=overwrite,
        ):
            written.append(service_name)

    _render_post_install_instructions(console, written)

    if written:
        console.print(
            f"\n[green]Done.[/green] {len(written)} service file(s) written.\n",
        )
    else:
        console.print("\n[yellow]No service files were written.[/yellow]\n")

    return 0
