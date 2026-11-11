"""Automation that deletes scheduled pending flows that have not started within
a certain time threshold.

These flows migth be pending due to a failure in the infrastructure or
deployments.
"""

from datetime import timedelta

from prefect.automations import Automation
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.events.schemas.events import ResourceSpecification

automation = Automation(
    name="Delete pending scheduled flows",
    description=(
        "Delete flows that have been scheduled but did not start within 12 hours. "
        "These flows might be pending due to a failure in the infrastructure or deployments."
    ),
    trigger=EventTrigger(
        after={"prefect.flow-run.Scheduled"},
        # Flow-run events are named after the *state name*
        # (prefect.flow-run.{state.name}); there is no "Started" state —
        # started runs emit ".Running".  With the old expectation
        # ("Started") the Proactive trigger never saw its expected event
        # and fired on every armed run after 12 hours, even ones that had
        # already completed, overwriting their terminal state with
        # CANCELLED (observed on manually triggered runs).
        expect={
            "prefect.flow-run.Running",
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
            state=StateType.CANCELLED,
            message="Flow was scheduled but did not start within 12 hours and is being cancelled.",
        ),
    ],
)
