"""Flow-related CLI subcommands."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, cast

from cyclopts import App
from cyclopts.exceptions import ValidationError
from rich.table import Table

from irsol_data_pipeline.cli.common import (
    get_console,
    print_json,
)
from irsol_data_pipeline.cli.metadata import (
    PREFECT_FLOW_GROUPS,
    OutputFormat,
    PrefectFlowGroupMetadata,
    PrefectFlowGroupName,
)

flows_app = App(name="flows", help="List and serve Prefect flow groups.")


def _flow_group_by_name() -> dict[PrefectFlowGroupName, PrefectFlowGroupMetadata]:
    """Build a lookup table for flow-group metadata.

    Returns:
        Mapping from canonical group name to metadata.
    """

    return {group.name: group for group in PREFECT_FLOW_GROUPS}


def _serialize_flow_groups(
    groups: Iterable[PrefectFlowGroupMetadata],
) -> list[dict[str, Any]]:
    """Convert flow metadata to stable JSON-serializable records.

    Args:
        groups: Flow-group metadata to serialize.

    Returns:
        List of serialized flow-group dictionaries.
    """

    return [
        {
            "description": group.description,
            "flows": [
                {
                    "automation": flow.automation,
                    "deployment_name": flow.deployment_name,
                    "description": flow.description,
                    "flow_name": flow.flow_name,
                    "schedule": flow.schedule,
                }
                for flow in group.flows
            ],
            "group": group.name,
            "topic_tag": group.topic_tag.value,
        }
        for group in groups
    ]


def _render_flow_groups_table(groups: Iterable[PrefectFlowGroupMetadata]) -> None:
    """Render a Rich table for flow-group metadata.

    Args:
        groups: Flow-group metadata to display.
    """

    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Group", style="white", no_wrap=True)
    table.add_column("Flow", style="white", no_wrap=True)
    table.add_column("Deployment", style="white", no_wrap=True)
    table.add_column("Automation", style="magenta", no_wrap=True)
    table.add_column("Schedule", style="green", no_wrap=True)
    table.add_column("Description", style="white")

    for group in groups:
        for flow in group.flows:
            table.add_row(
                group.name,
                flow.flow_name,
                flow.deployment_name,
                flow.automation,
                flow.schedule,
                flow.description,
            )

    get_console().print(table)


def _normalize_selected_groups(
    flow_groups: tuple[PrefectFlowGroupName, ...],
    *,
    all_groups: bool,
) -> tuple[PrefectFlowGroupMetadata, ...]:
    """Validate and normalize the requested flow groups.

    Args:
        flow_groups: Positional flow-group selections.
        all_groups: Whether all groups were requested.

    Returns:
        Normalized group metadata in CLI order.

    Raises:
        ValidationError: If the selection is invalid.
    """

    if all_groups and flow_groups:
        raise ValidationError("--all cannot be combined with explicit flow groups.")
    if not all_groups and not flow_groups:
        raise ValidationError(
            "Select at least one flow group or pass --all to serve all groups."
        )

    if all_groups:
        return PREFECT_FLOW_GROUPS

    seen: set[PrefectFlowGroupName] = set()
    group_lookup = _flow_group_by_name()
    selected = []
    for flow_group in flow_groups:
        if flow_group in seen:
            continue
        selected.append(group_lookup[flow_group])
        seen.add(flow_group)
    return tuple(selected)


def _build_flat_field_deployments() -> list[Any]:
    """Build flat-field correction deployments.

    Returns:
        Deployment objects for the flat-field correction group.
    """

    from prefect import serve

    del serve

    from irsol_data_pipeline.prefect.flows.flat_field_correction import (
        process_daily_unprocessed_measurements,
        process_unprocessed_measurements,
    )
    from irsol_data_pipeline.prefect.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        PrefectDeploymentTopicTag,
    )

    return [
        process_unprocessed_measurements.to_deployment(
            name="flat-field-correction-full",
            description=(
                "Run the flat field correction pipeline on all unprocessed "
                "measurements."
            ),
            cron="0 1 * * *",
            tags=[
                PrefectDeploymentTopicTag.FLAT_FIELD_CORRECTION.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
        process_daily_unprocessed_measurements.to_deployment(
            name="flat-field-correction-daily",
            description="Run the flat field correction pipeline on a specific day folder.",
            tags=[
                PrefectDeploymentTopicTag.FLAT_FIELD_CORRECTION.value,
                DeploymentAutomationTag.MANUAL.value,
            ],
        ),
    ]


def _build_slit_image_deployments() -> list[Any]:
    """Build slit-image generation deployments.

    Returns:
        Deployment objects for the slit-image group.
    """

    from irsol_data_pipeline.prefect.flows.slit_image_generation import (
        generate_daily_slit_images,
        generate_slit_images,
    )
    from irsol_data_pipeline.prefect.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        PrefectDeploymentTopicTag,
    )

    return [
        generate_slit_images.to_deployment(
            name="slit-images-full",
            description="Generate slit preview images for all unprocessed measurements.",
            cron="0 4 * * *",
            tags=[
                PrefectDeploymentTopicTag.SLIT_IMAGES.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
        generate_daily_slit_images.to_deployment(
            name="slit-images-daily",
            description="Generate slit preview images for a specific observation day.",
            tags=[
                PrefectDeploymentTopicTag.SLIT_IMAGES.value,
                DeploymentAutomationTag.MANUAL.value,
            ],
        ),
    ]


def _build_maintenance_deployments() -> list[Any]:
    """Build maintenance deployments.

    Returns:
        Deployment objects for the maintenance group.
    """

    from irsol_data_pipeline.prefect.flows.maintenance.delete_old_cache_files import (
        delete_old_cache_files,
    )
    from irsol_data_pipeline.prefect.flows.maintenance.delete_old_prefect_data import (
        delete_flow_runs_older_than,
    )
    from irsol_data_pipeline.prefect.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        PrefectDeploymentTopicTag,
    )

    delete_old_cache_files_flow = cast(Any, delete_old_cache_files)

    return [
        delete_flow_runs_older_than.to_deployment(
            name="prefect-run-cleanup",
            description="Delete Prefect flow runs older than a retention duration.",
            cron="0 0 * * *",
            tags=[
                PrefectDeploymentTopicTag.MAINTENANCE.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
        delete_old_cache_files_flow.to_deployment(
            name="cache-cleanup",
            description=(
                "Delete stale .pkl cache files under processed/_cache and "
                "processed/_sdo_cache."
            ),
            cron="30 0 * * *",
            tags=[
                PrefectDeploymentTopicTag.MAINTENANCE.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
    ]


def _build_web_assets_compatibility_deployments() -> list[Any]:
    """Build web-assets compatibility deployments.

    Returns:
        Deployment objects for the web-assets compatibility group.
    """

    from irsol_data_pipeline.prefect.flows.tags import (
        DeploymentAutomationTag,
        DeploymentScheduleTag,
        PrefectDeploymentTopicTag,
    )
    from irsol_data_pipeline.prefect.flows.web_assets_compatibility import (
        publish_web_assets_for_day,
        publish_web_assets_for_root,
    )

    return [
        publish_web_assets_for_root.to_deployment(
            name="web-assets-compatibility-full",
            description=(
                "Scan a dataset root and deploy legacy-compatible quicklook "
                "and context JPG assets for all days."
            ),
            cron="0 6 * * *",
            tags=[
                PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY.value,
                DeploymentScheduleTag.DAILY.value,
                DeploymentAutomationTag.SCHEDULED.value,
            ],
        ),
        publish_web_assets_for_day.to_deployment(
            name="web-assets-compatibility-daily",
            description=(
                "Deploy legacy-compatible quicklook and context JPG assets "
                "for a specific day folder."
            ),
            tags=[
                PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY.value,
                DeploymentAutomationTag.MANUAL.value,
            ],
        ),
    ]


def _build_deployments_for_group(group_name: PrefectFlowGroupName) -> list[Any]:
    """Build the deployments for a selected flow group.

    Args:
        group_name: Canonical flow-group name.

    Returns:
        Deployment objects for the requested group.
    """

    builders = {
        "flat-field-correction": _build_flat_field_deployments,
        "slit-images": _build_slit_image_deployments,
        "web-assets-compatibility": _build_web_assets_compatibility_deployments,
        "maintenance": _build_maintenance_deployments,
    }
    return builders[group_name]()


@flows_app.command(name="list")
def list_flows(
    topic: PrefectFlowGroupName | None = None,
    format: OutputFormat = "table",
) -> None:
    """List discoverable flow groups and their served deployments.

    Args:
        topic: Optional flow-group filter.
        format: Output format for the report.
    """

    groups = PREFECT_FLOW_GROUPS
    if topic is not None:
        groups = tuple(group for group in groups if group.name == topic)

    if format == "json":
        print_json({"flow_groups": _serialize_flow_groups(groups)})
        return

    _render_flow_groups_table(groups)


@flows_app.command(name="serve")
def serve_flows(
    *flow_groups: PrefectFlowGroupName,
    all: bool = False,
) -> None:
    """Register and serve one or more flow groups.

    Args:
        flow_groups: Flow groups to serve.
        all: Serve all available flow groups.
    """

    selected_groups = _normalize_selected_groups(flow_groups, all_groups=all)

    from prefect import serve

    deployments: list[Any] = []
    for group in selected_groups:
        deployments.extend(_build_deployments_for_group(group.name))

    serve(*deployments)
