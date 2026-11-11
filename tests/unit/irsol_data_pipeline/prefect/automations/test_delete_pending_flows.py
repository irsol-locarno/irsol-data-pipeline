"""Tests for the "Delete pending scheduled flows" automation.

The trigger's ``expect`` entries must be real Prefect state-change event
names.  Events are emitted as ``prefect.flow-run.{state.name}`` (see
``prefect.server.models.events.flow_run_state_change_event``), so every
expected event suffix must be an actual Prefect state name.  A wrong name
(e.g. the non-existent "Started") can never occur, which makes the
Proactive trigger fire on *every* armed run after 12 hours — including
runs that already completed — and overwrite their terminal state with
CANCELLED.
"""

from __future__ import annotations

from datetime import timedelta

from prefect.states import (
    Cancelled,
    Completed,
    Crashed,
    Failed,
    Pending,
    Running,
    Scheduled,
)

from irsol_data_pipeline.prefect.automations.delete_pending_flows import automation

# State names Prefect can actually emit as flow-run events.
_REAL_STATE_NAMES = {
    state().name
    for state in (Pending, Scheduled, Running, Cancelled, Crashed, Completed, Failed)
}


class TestDeletePendingFlowsAutomation:
    def test_expect_contains_only_real_state_names(self) -> None:
        expected_events = automation.trigger.expect
        assert expected_events, "trigger must expect some events"
        for event in expected_events:
            suffix = event.removeprefix("prefect.flow-run.")
            assert suffix in _REAL_STATE_NAMES, (
                f"{event!r} is not a Prefect state name; the event is never "
                "emitted and the Proactive trigger would fire on every run"
            )

    def test_expect_does_not_reference_started(self) -> None:
        # Regression guard: "Started" was never a Prefect state name
        # (runs emit ".Running"), which caused completed, manually
        # triggered runs to be cancelled 12 hours after scheduling.
        assert "prefect.flow-run.Started" not in automation.trigger.expect

    def test_started_runs_satisfy_the_trigger(self) -> None:
        # A run that starts emits Running; it must satisfy the trigger so
        # the automation only ever fires on runs that never started.
        assert "prefect.flow-run.Running" in automation.trigger.expect

    def test_trigger_configuration(self) -> None:
        assert automation.name == "Delete pending scheduled flows"
        assert automation.trigger.within == timedelta(hours=12)
        assert "prefect.flow-run.Scheduled" in automation.trigger.after
