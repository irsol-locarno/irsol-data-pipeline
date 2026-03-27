"""Automation implementation taken from https://docs.prefect.io/v3/advanced/detect-zombie-flows"""

from datetime import timedelta

from prefect.automations import Automation
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.events.schemas.events import ResourceSpecification

automation = Automation(
    name="Crash zombie flows",
    description=(
        "Mark flows that have not sent a heartbeat for 12 hours as crashed. "
        "These flows might be zombies due to a failure in the infrastructure or deployments."
    ),
    trigger=EventTrigger(
        after={"prefect.flow-run.Heartbeat"},
        expect={
            "prefect.flow-run.Heartbeat",
            "prefect.flow-run.Completed",
            "prefect.flow-run.Failed",
            "prefect.flow-run.Cancelled",
            "prefect.flow-run.Crashed",
        },
        match=ResourceSpecification({"prefect.resource.id": ["prefect.flow-run.*"]}),
        for_each={"prefect.resource.id"},
        posture=Posture.Proactive,
        threshold=1,
        within=timedelta(hours=12),
    ),
    actions=[
        ChangeFlowRunState(
            state=StateType.CRASHED,
            message="Flow did not send a heartbeat for 12 hours and is being marked as a zombie.",
        )
    ],
)
