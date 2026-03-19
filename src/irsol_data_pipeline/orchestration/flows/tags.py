from enum import StrEnum


class DeploymentTopicTag(StrEnum):
    FLAT_FIELD_CORRECTION = "flat-field-correction"
    SLIT_IMAGES = "slit-images"
    MAINTENANCE = "maintenance"


class DeploymentScheduleTag(StrEnum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class DeploymentAutomationTag(StrEnum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
