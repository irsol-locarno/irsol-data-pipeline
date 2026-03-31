"""Prefect status CLI subcommand."""

from __future__ import annotations

import asyncio

import requests
from pydantic import BaseModel, ConfigDict
from rich.table import Table

from irsol_data_pipeline.cli.common import get_console, print_json
from irsol_data_pipeline.cli.metadata import OutputFormat
from irsol_data_pipeline.prefect.config import (
    PREFECT_SERVER_HOST,
    PREFECT_SERVER_PORT,
    build_prefect_api_healthcheck_url,
    build_prefect_api_url,
    build_prefect_server_base_url,
)

DEFAULT_REQUEST_TIMEOUT_SECONDS = 5.0
PREFECT_TASK_RUN_PAGE_SIZE = 200


class PrefectStatusReport(BaseModel):
    """Operator-facing status information for the local Prefect server.

    Attributes:
        dashboard_url: Base URL for the dashboard.
        detail: Human-readable status detail.
        healthcheck_url: URL used for the HTTP probe.
        host: Expected Prefect host.
        http_status: HTTP status code when available.
        port: Expected Prefect port.
        reachable: Whether the server answered successfully.
        status: Stable machine-readable status string.
    """

    model_config = ConfigDict(frozen=True)

    dashboard_url: str
    detail: str
    healthcheck_url: str
    host: str
    http_status: int | None
    port: int
    reachable: bool
    status: str


class RunningFlowTaskSummary(BaseModel):
    """Running flow-run snapshot with associated running tasks.

    Attributes:
        flow_id: Flow identifier.
        flow_name: Human-readable flow name when available.
        flow_run_id: Flow-run identifier.
        flow_run_name: Human-readable flow-run name when available.
        running_task_count: Number of running tasks associated with the flow run.
        running_task_names: Running task run names associated with the flow run.
        state: Flow-run state label.
    """

    model_config = ConfigDict(frozen=True)

    flow_id: str
    flow_name: str
    flow_run_id: str
    flow_run_name: str
    running_task_count: int
    running_task_names: list[str]
    state: str


class PrefectDeepAnalysisReport(BaseModel):
    """Deep Prefect runtime analysis containing running flows and tasks."""

    model_config = ConfigDict(frozen=True)

    detail: str
    flow_run_count: int
    running_task_count: int
    running_flows: list[RunningFlowTaskSummary]


def _string_value(value: object, *, default: str = "-") -> str:
    """Normalize arbitrary JSON values to a printable string.

    Args:
        value: Value to stringify.
        default: Fallback when value is missing.

    Returns:
        Printable string value.
    """
    if isinstance(value, str) and value:
        return value
    return default


def _analyze_running_flows_and_tasks(
    host: str,
    port: int,
) -> PrefectDeepAnalysisReport:
    """Analyze currently running flow runs and their running tasks.

    Args:
        host: Prefect server host to probe.
        port: Prefect server port to probe.

    Returns:
        Deep-analysis report.
    """

    async def _collect() -> PrefectDeepAnalysisReport:
        from prefect.client.orchestration import get_client
        from prefect.client.schemas.filters import (
            FlowFilter,
            FlowFilterId,
            FlowRunFilter,
            FlowRunFilterState,
            FlowRunFilterStateType,
            TaskRunFilter,
            TaskRunFilterFlowRunId,
            TaskRunFilterState,
            TaskRunFilterStateName,
            TaskRunFilterStateType,
        )
        from prefect.client.schemas.sorting import FlowRunSort, TaskRunSort
        from prefect.settings import PREFECT_API_URL, temporary_settings

        api_base_url = build_prefect_api_url(host, port)

        with temporary_settings({PREFECT_API_URL: api_base_url}):
            async with get_client() as client:
                running_flow_runs = await client.read_flow_runs(
                    flow_run_filter=FlowRunFilter(
                        state=FlowRunFilterState(
                            type=FlowRunFilterStateType(any_=["RUNNING"]),
                        ),
                    ),
                    sort=FlowRunSort.EXPECTED_START_TIME_DESC,
                    limit=200,
                )

                if not running_flow_runs:
                    return PrefectDeepAnalysisReport(
                        detail="No running flow runs found.",
                        flow_run_count=0,
                        running_task_count=0,
                        running_flows=[],
                    )

                flow_run_ids = [str(flow_run.id) for flow_run in running_flow_runs]
                flow_ids = [str(flow_run.flow_id) for flow_run in running_flow_runs]

                flow_name_by_id: dict[str, str] = {}
                if flow_ids:
                    flow_records = await client.read_flows(
                        flow_filter=FlowFilter(id=FlowFilterId(any_=flow_ids)),
                        limit=len(flow_ids),
                    )
                    flow_name_by_id = {
                        str(flow.id): _string_value(
                            getattr(flow, "name", None),
                            default="unknown-flow",
                        )
                        for flow in flow_records
                    }

                running_tasks = []
                offset = 0
                while True:
                    task_page = await client.read_task_runs(
                        task_run_filter=TaskRunFilter(
                            state=TaskRunFilterState(
                                type=TaskRunFilterStateType(any_=["RUNNING"]),
                                name=TaskRunFilterStateName(any_=["Running"]),
                            ),
                            flow_run_id=TaskRunFilterFlowRunId(any_=flow_run_ids),
                        ),
                        sort=TaskRunSort.EXPECTED_START_TIME_DESC,
                        limit=PREFECT_TASK_RUN_PAGE_SIZE,
                        offset=offset,
                    )
                    if not task_page:
                        break
                    running_tasks.extend(task_page)
                    if len(task_page) < PREFECT_TASK_RUN_PAGE_SIZE:
                        break
                    offset += PREFECT_TASK_RUN_PAGE_SIZE

                task_names_by_flow_run: dict[str, list[str]] = {
                    flow_run_id: [] for flow_run_id in flow_run_ids
                }
                for task_run in running_tasks:
                    flow_run_id = str(task_run.flow_run_id)
                    if flow_run_id not in task_names_by_flow_run:
                        continue
                    task_name = _string_value(
                        getattr(task_run, "name", None),
                        default="unnamed-task",
                    )
                    task_names_by_flow_run[flow_run_id].append(task_name)

                running_flow_summaries: list[RunningFlowTaskSummary] = []
                for flow_run in running_flow_runs:
                    flow_run_id = str(flow_run.id)
                    flow_id = str(flow_run.flow_id)
                    running_task_names = task_names_by_flow_run.get(flow_run_id, [])
                    running_flow_summaries.append(
                        RunningFlowTaskSummary(
                            flow_id=flow_id,
                            flow_name=flow_name_by_id.get(flow_id, "unknown-flow"),
                            flow_run_id=flow_run_id,
                            flow_run_name=_string_value(
                                getattr(flow_run, "name", None),
                                default="unnamed-flow-run",
                            ),
                            running_task_count=len(running_task_names),
                            running_task_names=sorted(running_task_names),
                            state=_string_value(
                                getattr(flow_run, "state_name", None),
                                default="RUNNING",
                            ),
                        ),
                    )

                running_flow_summaries.sort(
                    key=lambda item: (
                        item.running_task_count,
                        item.flow_name,
                        item.flow_run_name,
                    ),
                    reverse=True,
                )

                running_task_count = sum(
                    summary.running_task_count for summary in running_flow_summaries
                )
                return PrefectDeepAnalysisReport(
                    detail="Collected running flow and task details from Prefect SDK.",
                    flow_run_count=len(running_flow_summaries),
                    running_task_count=running_task_count,
                    running_flows=running_flow_summaries,
                )

    try:
        return asyncio.run(_collect())
    except RuntimeError:
        # Keep CLI behavior robust in environments where an event loop already exists.
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_collect())
        finally:
            loop.close()


def _check_prefect_status(host: str, port: int) -> PrefectStatusReport:
    """Probe the local Prefect API health endpoint.

    Args:
        host: Prefect server host to probe.
        port: Prefect server port to probe.

    Returns:
        Structured status report for the expected local Prefect server.
    """
    dashboard_url = build_prefect_server_base_url(host, port)
    healthcheck_url = build_prefect_api_healthcheck_url(host, port)

    try:
        response = requests.get(
            healthcheck_url,
            timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return PrefectStatusReport(
            dashboard_url=dashboard_url,
            detail=str(exc),
            healthcheck_url=healthcheck_url,
            host=host,
            http_status=None,
            port=port,
            reachable=False,
            status="unreachable",
        )

    if response.ok:
        return PrefectStatusReport(
            dashboard_url=dashboard_url,
            detail="Prefect dashboard is reachable on the expected port.",
            healthcheck_url=healthcheck_url,
            host=host,
            http_status=response.status_code,
            port=port,
            reachable=True,
            status="running",
        )

    return PrefectStatusReport(
        dashboard_url=dashboard_url,
        detail=f"Health check returned HTTP {response.status_code}.",
        healthcheck_url=healthcheck_url,
        host=host,
        http_status=response.status_code,
        port=port,
        reachable=False,
        status="error",
    )


def _render_status_report(report: PrefectStatusReport) -> None:
    """Render the human-readable Prefect status table.

    Args:
        report: Status report to display.
    """
    title_style = "bold italic green" if report.reachable else "bold italic red"
    header_style = "bold green" if report.reachable else "bold red"
    status_style = "green" if report.reachable else "red"
    table = Table(
        title="Prefect Status",
        show_header=True,
        title_style=title_style,
        header_style=header_style,
    )
    table.add_column("Field", style="white", no_wrap=True)
    table.add_column("Value", style="white")
    table.add_row("Status", f"[{status_style}]{report.status}[/{status_style}]")
    table.add_row("Reachable", "yes" if report.reachable else "no")
    table.add_row("Host", report.host)
    table.add_row("Port", str(report.port))
    table.add_row("Dashboard URL", report.dashboard_url)
    table.add_row("Health Check", report.healthcheck_url)
    table.add_row(
        "HTTP Status",
        str(report.http_status) if report.http_status is not None else "-",
    )
    table.add_row("Detail", report.detail)
    get_console().print(table)


def _render_deep_analysis_report(report: PrefectDeepAnalysisReport) -> None:
    """Render running flow/task deep-analysis information.

    Args:
        report: Deep-analysis report to display.
    """
    summary_table = Table(
        title="Prefect Deep Analysis",
        show_header=True,
        header_style="bold cyan",
    )
    summary_table.add_column("Field", style="white", no_wrap=True)
    summary_table.add_column("Value", style="white")
    summary_table.add_row("Flow Runs", str(report.flow_run_count))
    summary_table.add_row("Running Tasks", str(report.running_task_count))
    summary_table.add_row("Detail", report.detail)
    get_console().print(summary_table)

    flow_table = Table(
        title="Running Flows and Tasks",
        show_header=True,
        header_style="bold cyan",
    )
    flow_table.add_column("Flow", style="white", no_wrap=True)
    flow_table.add_column("Flow Run", style="white")
    flow_table.add_column("State", style="magenta", no_wrap=True)
    flow_table.add_column("Running Tasks", style="green", no_wrap=True)
    flow_table.add_column("Task Names", style="white")

    if not report.running_flows:
        flow_table.add_row("-", "-", "-", "0", "No running flows")
    else:
        for entry in report.running_flows:
            flow_table.add_row(
                entry.flow_name,
                entry.flow_run_name,
                entry.state,
                str(entry.running_task_count),
                "\n".join(entry.running_task_names)
                if entry.running_task_names
                else "-",
            )

    get_console().print(flow_table)


def status(
    format: OutputFormat = "table",
    host: str = PREFECT_SERVER_HOST,
    port: int = PREFECT_SERVER_PORT,
    deep_analysis: bool = False,
) -> int:
    """Check whether the local Prefect dashboard is reachable on its expected
    port.

    Args:
        format: Output format for the report.
        host: Prefect server host to probe.
        port: Prefect server port to probe.
        deep_analysis: Fetch running flow/task details from Prefect API.

    Returns:
        Zero when the local Prefect dashboard is reachable, otherwise one.
    """
    report = _check_prefect_status(host=host, port=port)
    payload: dict[str, object] = report.model_dump()
    deep_report: PrefectDeepAnalysisReport | None = None
    if deep_analysis and report.reachable:
        try:
            deep_report = _analyze_running_flows_and_tasks(host=host, port=port)
            payload["deep_analysis"] = deep_report.model_dump()
        except Exception as exc:
            payload["deep_analysis"] = {
                "detail": f"Failed to run deep analysis: {exc}",
                "flow_run_count": 0,
                "running_task_count": 0,
                "running_flows": [],
            }

    if format == "json":
        print_json(payload)
    else:
        _render_status_report(report)
        if deep_analysis:
            if deep_report is not None:
                _render_deep_analysis_report(deep_report)
            elif "deep_analysis" in payload:
                fallback = payload["deep_analysis"]
                if isinstance(fallback, dict):
                    _render_deep_analysis_report(
                        PrefectDeepAnalysisReport(
                            detail=str(
                                fallback.get("detail", "Deep analysis unavailable."),
                            ),
                            flow_run_count=0,
                            running_task_count=0,
                            running_flows=[],
                        ),
                    )

    return 0 if report.reachable else 1
