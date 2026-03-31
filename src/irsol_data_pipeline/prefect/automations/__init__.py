from prefect.automations import Automation

from .delete_pending_flows import automation as delete_pending_flows_automation
from .zombie_flows import automation as zombie_flow_automation

AUTOMATIONS = (zombie_flow_automation, delete_pending_flows_automation)

__all__ = [
    "AUTOMATIONS",
    "delete_pending_flows_automation",
    "get_automation",
    "zombie_flow_automation",
]


def get_automation(name: str) -> Automation | None:
    try:
        automation: Automation = Automation.read(name=name)
    except Exception:
        return None
    return automation
