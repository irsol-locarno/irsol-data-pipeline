"""Static command metadata and registries for the CLI."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from irsol_data_pipeline.core.config import (
    DEFAULT_PIOMBO_BASE_PATH,
    DEFAULT_PIOMBO_HOST_NAME,
)
from irsol_data_pipeline.prefect.flows.tags import PrefectDeploymentTopicTag
from irsol_data_pipeline.prefect.variables import PrefectVariableName

OutputFormat = Literal["table", "json"]
PrefectFlowGroupName = Literal[
    "flat-field-correction",
    "slit-images",
    "web-assets-compatibility",
    "maintenance",
]


@dataclass(frozen=True)
class PrefectVariableMetadata:
    """Metadata describing one configurable Prefect variable.

    Attributes:
        prefect_name: Canonical Prefect variable name.
        prompt_text: Prompt displayed during interactive configuration.
        default_value: Optional default shown to operators.
        required: Whether the variable must be set for normal operation.
        topic_tags: Flow groups that depend on this variable.
    """

    prefect_name: PrefectVariableName
    prompt_text: str
    default_value: str | None = None
    required: bool = True
    topic_tags: tuple[PrefectDeploymentTopicTag, ...] = ()


@dataclass(frozen=True)
class PrefectFlowMetadata:
    """Metadata describing one flow/deployment exposed by the CLI.

    Attributes:
        group_name: User-facing flow-group identifier.
        flow_name: Registered Prefect flow name.
        deployment_name: Deployment name created by `flows serve`.
        description: Operator-facing description.
        automation: Automation mode tag.
        schedule: Schedule label shown in reports.
    """

    group_name: PrefectFlowGroupName
    flow_name: str
    deployment_name: str
    description: str
    automation: Literal["manual", "scheduled"]
    schedule: str


@dataclass(frozen=True)
class PrefectFlowGroupMetadata:
    """Metadata describing one flow group.

    Attributes:
        name: Canonical CLI flow-group name.
        topic_tag: Deployment topic tag associated with the group.
        description: Human-readable group summary.
        flows: Concrete flow metadata entries served by the group.
    """

    name: PrefectFlowGroupName
    topic_tag: PrefectDeploymentTopicTag
    description: str
    flows: tuple[PrefectFlowMetadata, ...] = field(default_factory=tuple)


PREFECT_VARIABLES: tuple[PrefectVariableMetadata, ...] = (
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.DATA_ROOT_PATH,
        prompt_text="Default dataset root path used by Prefect flows",
        required=True,
        topic_tags=(
            PrefectDeploymentTopicTag.FLAT_FIELD_CORRECTION,
            PrefectDeploymentTopicTag.SLIT_IMAGES,
            PrefectDeploymentTopicTag.MAINTENANCE,
        ),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.JSOC_EMAIL,
        prompt_text=(
            "JSOC email (register at http://jsoc.stanford.edu/ajax/register_email.html)"
        ),
        required=True,
        topic_tags=(PrefectDeploymentTopicTag.SLIT_IMAGES,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.JSOC_DATA_DELAY_DAYS,
        prompt_text=(
            "Minimum age in days for slit-image day scanning (JSOC data delay)"
        ),
        default_value="14",
        required=False,
        topic_tags=(PrefectDeploymentTopicTag.SLIT_IMAGES,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.CACHE_EXPIRATION_HOURS,
        prompt_text="Cache expiration time in hours (e.g. 4 weeks)",
        default_value=f"{24 * 7 * 4}",
        required=False,
        topic_tags=(PrefectDeploymentTopicTag.MAINTENANCE,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.FLOW_RUN_EXPIRATION_HOURS,
        prompt_text="Prefect flow-run history retention in hours (e.g. 4 weeks)",
        default_value=f"{24 * 7 * 4}",
        required=False,
        topic_tags=(PrefectDeploymentTopicTag.MAINTENANCE,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.PIOMBO_BASE_PATH,
        prompt_text="Base path for Piombo web-assets uploads",
        default_value=DEFAULT_PIOMBO_BASE_PATH,
        required=False,
        topic_tags=(PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.PIOMBO_HOSTNAME,
        prompt_text="SSH hostname used by Piombo for web-assets upload",
        required=False,
        default_value=DEFAULT_PIOMBO_HOST_NAME,
        topic_tags=(PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.PIOMBO_USERNAME,
        prompt_text="SSH username used by Piombo for web-assets upload",
        required=True,
        topic_tags=(PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY,),
    ),
    PrefectVariableMetadata(
        prefect_name=PrefectVariableName.PIOMBO_PASSWORD,
        prompt_text="SSH password used by Piombo for web-assets upload",
        required=True,
        topic_tags=(PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY,),
    ),
)


PREFECT_FLOW_GROUPS: tuple[PrefectFlowGroupMetadata, ...] = (
    PrefectFlowGroupMetadata(
        name="flat-field-correction",
        topic_tag=PrefectDeploymentTopicTag.FLAT_FIELD_CORRECTION,
        description="Serve the flat-field correction deployments.",
        flows=(
            PrefectFlowMetadata(
                group_name="flat-field-correction",
                flow_name="ff-correction-full",
                deployment_name="flat-field-correction-full",
                description=(
                    "Run the flat field correction pipeline on all unprocessed "
                    "measurements."
                ),
                automation="scheduled",
                schedule="daily",
            ),
            PrefectFlowMetadata(
                group_name="flat-field-correction",
                flow_name="ff-correction-daily",
                deployment_name="flat-field-correction-daily",
                description="Run the flat field correction pipeline on a specific day folder.",
                automation="manual",
                schedule="manual",
            ),
        ),
    ),
    PrefectFlowGroupMetadata(
        name="slit-images",
        topic_tag=PrefectDeploymentTopicTag.SLIT_IMAGES,
        description="Serve the slit-image generation deployments.",
        flows=(
            PrefectFlowMetadata(
                group_name="slit-images",
                flow_name="slit-images-full",
                deployment_name="slit-images-full",
                description=(
                    "Generate slit preview images for all unprocessed measurements."
                ),
                automation="scheduled",
                schedule="daily",
            ),
            PrefectFlowMetadata(
                group_name="slit-images",
                flow_name="slit-images-daily",
                deployment_name="slit-images-daily",
                description=(
                    "Generate slit preview images for a specific observation day."
                ),
                automation="manual",
                schedule="manual",
            ),
        ),
    ),
    PrefectFlowGroupMetadata(
        name="web-assets-compatibility",
        topic_tag=PrefectDeploymentTopicTag.WEB_ASSETS_COMPATIBILITY,
        description="Serve compatibility deployments for quicklook/context assets.",
        flows=(
            PrefectFlowMetadata(
                group_name="web-assets-compatibility",
                flow_name="web-assets-compatibility-full",
                deployment_name="web-assets-compatibility-full",
                description=(
                    "Scan a dataset root and deploy quicklook "
                    "and context JPG assets for all days."
                ),
                automation="scheduled",
                schedule="daily",
            ),
            PrefectFlowMetadata(
                group_name="web-assets-compatibility",
                flow_name="web-assets-compatibility-daily",
                deployment_name="web-assets-compatibility-daily",
                description=(
                    "Deploy quicklook and context JPG assets for a specific day folder."
                ),
                automation="manual",
                schedule="manual",
            ),
        ),
    ),
    PrefectFlowGroupMetadata(
        name="maintenance",
        topic_tag=PrefectDeploymentTopicTag.MAINTENANCE,
        description="Serve maintenance and retention-cleanup deployments.",
        flows=(
            PrefectFlowMetadata(
                group_name="maintenance",
                flow_name="maintenance-cleanup",
                deployment_name="prefect-run-cleanup",
                description="Delete Prefect flow runs older than a retention duration.",
                automation="scheduled",
                schedule="daily",
            ),
            PrefectFlowMetadata(
                group_name="maintenance",
                flow_name="maintenance-cache-cleanup",
                deployment_name="cache-cleanup",
                description=(
                    "Delete stale .pkl cache files under processed/_cache and "
                    "processed/_sdo_cache."
                ),
                automation="scheduled",
                schedule="daily",
            ),
        ),
    ),
)
