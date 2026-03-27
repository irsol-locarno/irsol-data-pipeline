# Automations

## Overview

The IRSOL Data Pipeline includes built-in **Prefect Automations** to ensure reliable and self-healing orchestration. Automations monitor flow run states and take action when flows are stuck, delayed, or unresponsive.

## Available Automations

- **Delete Pending Scheduled Flows:**
  - Cancels flows that remain in the `Scheduled` state for too long without starting.
  - Prevents buildup of stuck or orphaned runs due to infrastructure issues.
- **Crash Zombie Flows:**
  - Marks flows as `Crashed` if they do not send a heartbeat for an extended period.
  - Helps identify and clean up zombie processes.

## How to Use

Automations are registered and updated automatically when you run:

```bash
idp configure
```

This ensures the Prefect server is always running the latest automation rules. You can customize or add new automations by editing the files in `src/irsol_data_pipeline/prefect/automations/` and re-running the command.

## See Also
- [Prefect Automations Concept](../prefect/automations.md)
- [Prefect Operations](../maintainer/prefect_operations.md)
