from prefect.automations import Automation

from .delete_pending_flows import automation as delete_pending_flows_automation
from .zombie_flows import automation as zombie_flow_automation

AUTOMATIONS = (zombie_flow_automation, delete_pending_flows_automation)

__all__ = [
    "zombie_flow_automation",
    "delete_pending_flows_automation",
    "AUTOMATIONS",
    "get_automation",
]


def get_automation(name: str) -> Automation | None:
    try:
        automation: Automation = Automation.read(name=name)  # noqa
    except Exception:
        return None
    return automation
