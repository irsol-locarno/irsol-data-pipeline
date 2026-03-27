# Prefect Automations

The IRSOL Data Pipeline leverages **Prefect Automations** to monitor and manage the state of flow runs, ensuring robust and reliable orchestration. Automations are event-driven rules that can take actions (such as cancelling or marking flows) based on flow run state transitions and timing conditions.

## What Are Automations?
Automations in Prefect are proactive rules that listen for specific events (e.g., a flow run remaining in a certain state for too long) and automatically trigger actions. This helps maintain a healthy workflow environment by cleaning up stuck or zombie flows without manual intervention.

## Built-in Automations

### 1. Delete Pending Scheduled Flows
- **Purpose:** Cancels flows that remain in the `Scheduled` state (or similar) for too long without starting.
- **Trigger:** If a flow run is `Scheduled` and does not transition to `Started` or `Cancelled` within a set time window (default: 12 hours, configurable for testing).
- **Action:** The flow run is automatically cancelled with a message indicating it did not start in time.

### 2. Crash Zombie Flows
- **Purpose:** Marks flows as `Crashed` if they do not send a heartbeat for an extended period (default: 12 hours).
- **Trigger:** If a running flow does not emit a heartbeat event within the threshold.
- **Action:** The flow run is marked as `Crashed` to indicate a likely infrastructure or deployment failure.

## Future automations
There's potential to add more automations in the future, such as:
- Notification automation for flows that fail or crash, sending alerts to maintainers (see https://docs.prefect.io/v3/api-ref/python/prefect-blocks-notifications)

## How It Works
Automations are defined in the `src/irsol_data_pipeline/prefect/automations/` directory and registered via the CLI (`idp configure`). They use Prefect's `EventTrigger` and `ChangeFlowRunState` to specify the conditions and actions.

## Customization
You can adjust the time thresholds or add new automations by editing the relevant Python files in the automations directory. After making changes, re-run `idp configure` to update the registered automations.


See the [CLI documentation](../cli/cli_usage.md#automations) for usage details.
