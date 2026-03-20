import enum


class PrefectDeploymentTopicTag(enum.Enum):
    FLAT_FIELD_CORRECTION = "flat-field-correction"
    SLIT_IMAGES = "slit-images"
    MAINTENANCE = "maintenance"


class DeploymentScheduleTag(enum.Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class DeploymentAutomationTag(enum.Enum):
    SCHEDULED = "scheduled"
    MANUAL = "manual"
