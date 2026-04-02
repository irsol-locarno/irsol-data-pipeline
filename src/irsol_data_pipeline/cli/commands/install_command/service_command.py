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
_DEFAULT_USER = "operator"
_DEFAULT_WORKING_DIRECTORY: Path = Path(f"/home/{_DEFAULT_USER}")

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

    When this command is executed as root (required to write to
    ``/etc/systemd/system``), the auto-detected path may be wrong or
    missing because root's ``PATH`` does not include the target user's
    virtual environment.  The caller should supply the full absolute path
    from the target user's environment (e.g.
    ``/home/<user>/.venv/bin/idp``).

    Args:
        console: Rich console for output.

    Returns:
        Absolute path string to the ``idp`` binary.
    """
    detected = _detect_idp_path()
    return Prompt.ask(
        "Full path to the [bold]idp[/bold] executable\n"
        "  [dim](if running as root, provide the path from the target "
        "user's environment, e.g. /home/<user>/.venv/bin/idp)[/dim]",
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


def _prompt_working_directory(console: Console, username: str | None = None) -> Path:
    """Prompt for the working directory used by the services.

    This directory becomes the CWD of each service process and is the
    location where any relative log or stderr paths will resolve to.

    When *username* is provided the default is ``/home/<username>``;
    otherwise ``_DEFAULT_WORKING_DIRECTORY`` is used as the fallback.

    Args:
        console: Rich console for output.
        username: Unix username to derive the default working directory from.

    Returns:
        Working directory as a Path.
    """
    default = f"/home/{username}" if username else str(_DEFAULT_WORKING_DIRECTORY)
    raw = Prompt.ask(
        "Working directory for the services",
        default=default,
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


def _generate_server_unit(user: str, idp_path: str, working_directory: str) -> str:
    """Render the Prefect server systemd unit file.

    Args:
        user: Unix user for the service.
        idp_path: Absolute path to the ``idp`` executable.
        working_directory: Working directory for the service process.

    Returns:
        Rendered unit file content.
    """
    template = _load_template(_SERVER_TEMPLATE_NAME)
    return template.substitute(
        user=user,
        idp_executable_path=idp_path,
        working_directory=working_directory,
    )


def _generate_flow_runner_unit(
    user: str,
    idp_path: str,
    flow_group_name: str,
    working_directory: str,
) -> str:
    """Render a flow-runner systemd unit file.

    Args:
        user: Unix user for the service.
        idp_path: Absolute path to the ``idp`` executable.
        flow_group_name: Canonical flow-group name.
        working_directory: Working directory for the service process.

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
        working_directory=working_directory,
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


def _stop_service(console: Console, service_name: str) -> None:
    """Stop a running systemd service.

    Args:
        console: Rich console for output.
        service_name: Systemd unit name.
    """
    result = subprocess.run(  # noqa: S603
        ["systemctl", "stop", service_name],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        console.print(f"  [green]✓[/green] Stopped [bold]{service_name}[/bold]")
    else:
        console.print(
            f"  [dim]Could not stop {service_name} (may not be running)[/dim]"
        )


def _disable_service(console: Console, service_name: str) -> None:
    """Disable a systemd service from starting at boot.

    Args:
        console: Rich console for output.
        service_name: Systemd unit name.
    """
    result = subprocess.run(  # noqa: S603
        ["systemctl", "disable", service_name],  # noqa: S607
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        console.print(f"  [green]✓[/green] Disabled [bold]{service_name}[/bold]")
    else:
        console.print(
            f"  [dim]Could not disable {service_name} (may not be enabled)[/dim]"
        )


def _remove_unit_file(
    console: Console,
    systemd_dir: Path,
    service_name: str,
) -> bool:
    """Remove a systemd unit file from disk.

    Args:
        console: Rich console for output.
        systemd_dir: Directory where systemd unit files reside.
        service_name: Service file name.

    Returns:
        True when the file was removed.
    """
    target = systemd_dir / service_name
    if not target.exists():
        console.print(f"  [dim]{service_name} not found on disk, skipping[/dim]")
        return False
    target.unlink()
    console.print(f"  [green]✓[/green] Removed [bold]{target}[/bold]")
    return True


def _prompt_services_to_uninstall(
    console: Console,
    status: dict[str, bool],
) -> list[str]:
    """Prompt the user to select which installed services to uninstall.

    Args:
        console: Rich console for output.
        status: Mapping of service name to installation status.

    Returns:
        List of service names selected for removal.
    """
    installed = [name for name, present in status.items() if present]
    selected: list[str] = []
    for service_name in installed:
        if Confirm.ask(
            f"Uninstall [bold]{service_name}[/bold]?",
            default=True,
            console=console,
        ):
            selected.append(service_name)
    return selected


def _render_post_uninstall_instructions(
    console: Console,
    removed_services: list[str],
) -> None:
    """Render post-uninstallation instructions.

    Args:
        console: Rich console for output.
        removed_services: List of service file names that were removed.
    """
    if not removed_services:
        return

    console.print()
    console.print(
        Panel(
            "sudo systemctl daemon-reload",
            title="Next steps",
            subtitle="Run this command to reload the systemd configuration",
            border_style="green",
        ),
    )


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
    working_dir = _prompt_working_directory(console, user)

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
        content = _generate_server_unit(user, idp_path, str(working_dir))
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
        content = _generate_flow_runner_unit(
            user, idp_path, group_name, str(working_dir)
        )
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


def uninstall_service() -> int:
    """Interactively stop, disable, and remove installed systemd service unit
    files.

    Detects which pipeline services are currently installed, prompts the user
    to select which ones to remove, stops and disables each selected service,
    and deletes the corresponding unit files from disk.

    Returns:
        Exit code for the command.
    """
    console = Console()

    console.print()
    console.print(
        Panel(
            "[bold]IRSOL Data Pipeline — Service Uninstaller[/bold]\n\n"
            "This command stops, disables, and removes systemd service unit\n"
            "files for the Prefect server and flow runners.",
            border_style="red",
        ),
    )
    console.print()

    systemd_dir = _prompt_systemd_dir(console)

    existing = _detect_existing_services(console, systemd_dir)
    _render_existing_services(console, existing)

    installed_count = sum(1 for v in existing.values() if v)
    if installed_count == 0:
        console.print(
            "[yellow]No installed pipeline services found. Nothing to do.[/yellow]\n"
        )
        return 0

    selected = _prompt_services_to_uninstall(console, existing)

    if not selected:
        console.print("\n[yellow]No services selected. Nothing to do.[/yellow]\n")
        return 0

    console.print()
    removed: list[str] = []
    for service_name in selected:
        console.print(f"\n[bold]{service_name}[/bold]")
        _stop_service(console, service_name)
        _disable_service(console, service_name)
        if _remove_unit_file(console, systemd_dir, service_name):
            removed.append(service_name)

    _render_post_uninstall_instructions(console, removed)

    if removed:
        console.print(
            f"\n[green]Done.[/green] {len(removed)} service file(s) removed.\n",
        )
    else:
        console.print("\n[yellow]No service files were removed.[/yellow]\n")

    return 0
