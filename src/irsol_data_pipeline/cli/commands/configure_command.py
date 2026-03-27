"""Prefect profile configuration CLI command."""

from __future__ import annotations

from pathlib import Path

from prefect.settings import (
    PREFECT_API_DATABASE_CONNECTION_URL,
    PREFECT_API_URL,
    PREFECT_RESULTS_PERSIST_BY_DEFAULT,
    PREFECT_SERVER_ANALYTICS_ENABLED,
    PREFECT_TASKS_DEFAULT_PERSIST_RESULT,
    Profile,
    load_profiles,
    save_profiles,
)

from irsol_data_pipeline.prefect.config import (
    PREFECT_SERVER_HOST,
    PREFECT_SERVER_PORT,
    build_prefect_api_url,
)

DEFAULT_PREFECT_PROFILE_NAME = "default"
DEFAULT_PREFECT_DATABASE_PATH = Path("/dati/.prefect/prefect.db")


def _confirm(prompt_text: str, *, default: bool = False) -> bool:
    """Prompt for a yes/no confirmation.

    Args:
        prompt_text: Prompt displayed to the operator.
        default: Default confirmation value used when no input is provided.

    Returns:
        True when the operator confirmed.
    """

    response = input(prompt_text).strip().lower()
    if not response:
        return default
    return response in {"y", "yes"}


def _build_sqlite_connection_url(database_path: Path) -> str:
    """Build the sqlite connection URL expected by Prefect.

    Args:
        database_path: Filesystem path to ``prefect.db``.

    Returns:
        Sqlite connection URL.
    """

    return f"sqlite+aiosqlite:///{database_path.expanduser().resolve().as_posix()}"


def _prompt_database_path() -> Path:
    """Prompt for the Prefect database path.

    Returns:
        Selected path to the Prefect sqlite database.
    """

    if _confirm(
        (
            "Use default Prefect database location "
            f"'{DEFAULT_PREFECT_DATABASE_PATH}'? [Y/n]: "
        ),
        default=True,
    ):
        DEFAULT_PREFECT_DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
        return DEFAULT_PREFECT_DATABASE_PATH

    while True:
        raw_path = input("Enter Prefect database file path: ").strip()
        if raw_path:
            custom_path = Path(raw_path).expanduser().resolve()
            custom_path.parent.mkdir(parents=True, exist_ok=True)
            return custom_path
        print("  x Database path is required.")


def _prompt_api_port() -> int:
    """Prompt for the Prefect API port.

    Returns:
        Selected API port.
    """

    while True:
        raw_port = input(f"Prefect API port [{PREFECT_SERVER_PORT}]: ").strip()
        if not raw_port:
            return PREFECT_SERVER_PORT

        try:
            port = int(raw_port)
        except ValueError:
            print("  x Port must be an integer.")
            continue

        if 1 <= port <= 65535:
            return port

        print("  x Port must be in range 1-65535.")


def _manage_automations():

    from prefect.automations import Automation

    from irsol_data_pipeline.prefect.automations import (
        delete_pending_flows_automation,
        zombie_flow_automation,
    )

    automations: list[Automation] = [
        zombie_flow_automation,
        delete_pending_flows_automation,
    ]
    for i, automation in enumerate(automations, start=1):
        print(f"{i}/{len(automations)}) Registering automation '{automation.name}'")
        try:
            existing_automation: Automation = Automation.read(name=automation.name)  # noqa
        except Exception:
            automation.create()
            print(
                f"{i}/{len(automations)}) Automation '{automation.name}' registered successfully."
            )
        else:
            print(
                f"{i}/{len(automations)}) Automation '{automation.name}' already exists. Updating it."
            )
            existing_automation.name = automation.name
            existing_automation.description = automation.description
            existing_automation.trigger = automation.trigger
            existing_automation.actions = automation.actions
            existing_automation.update()
            print(
                f"{i}/{len(automations)}) Automation '{automation.name}' updated successfully."
            )


def configure_prefect() -> int:
    """Create or update the IRSOL data pipeline and prefect server.

    Returns:
        Exit code for the command.
    """

    print("Prefect Profile Configuration\n")

    database_path = _prompt_database_path()
    api_port = _prompt_api_port()
    database_connection_url = _build_sqlite_connection_url(database_path)
    api_url = build_prefect_api_url(PREFECT_SERVER_HOST, api_port)

    settings = {
        PREFECT_API_DATABASE_CONNECTION_URL: database_connection_url,
        PREFECT_API_URL: api_url,
        PREFECT_SERVER_ANALYTICS_ENABLED: False,
        PREFECT_RESULTS_PERSIST_BY_DEFAULT: False,
        PREFECT_TASKS_DEFAULT_PERSIST_RESULT: False,
    }

    profiles = load_profiles()
    profile_exists = DEFAULT_PREFECT_PROFILE_NAME in profiles.names

    if profile_exists:
        profiles.update_profile(DEFAULT_PREFECT_PROFILE_NAME, settings)
    else:
        profiles.add_profile(
            Profile(name=DEFAULT_PREFECT_PROFILE_NAME, settings=settings)
        )

    profiles.set_active(DEFAULT_PREFECT_PROFILE_NAME)
    save_profiles(profiles)

    action = "Updated" if profile_exists else "Created"
    print(f"{action} Prefect profile '{DEFAULT_PREFECT_PROFILE_NAME}'.")
    print(f"  PREFECT_API_DATABASE_CONNECTION_URL={database_connection_url}")
    print(f"  PREFECT_API_URL={api_url}")
    print("  PREFECT_SERVER_ANALYTICS_ENABLED=false")
    print("  PREFECT_RESULTS_PERSIST_BY_DEFAULT=false")
    print("  PREFECT_TASKS_DEFAULT_PERSIST_RESULT=false")

    _manage_automations()
    return 0
