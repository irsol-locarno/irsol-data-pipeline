"""Domain models for web-asset compatibility."""

from __future__ import annotations

import enum
from pathlib import Path, PurePosixPath

from pydantic import BaseModel, ConfigDict


class WebAssetKind(enum.Enum):
    QUICK_LOOK = "quicklook"
    CONTEXT = "context"


class WebAssetFolderName(enum.Enum):
    """Folder names used by legacy web assets on Piombo."""

    QUICK_LOOK = "img_quicklook"
    CONTEXT = "img_data"

    @classmethod
    def for_asset_kind(cls, kind: WebAssetKind) -> WebAssetFolderName:
        return {
            WebAssetKind.QUICK_LOOK: cls.QUICK_LOOK,
            WebAssetKind.CONTEXT: cls.CONTEXT,
        }[kind]


class WebAssetSource(BaseModel):
    """One generated PNG that can be deployed as a compatible JPG.

    Attributes:
        kind: Destination bucket (`quicklook` or `context`).
        observation_name: Observation day folder name (YYMMDD).
        measurement_name: Canonical measurement name.
        source_path: Path to the generated PNG source file.
    """

    model_config = ConfigDict(frozen=True)

    kind: WebAssetKind
    observation_name: str
    measurement_name: str
    source_path: Path

    @property
    def remote_target_path(self) -> str:
        """Compute the POSIX remote target path for this asset.

        Returns:
            Relative POSIX path of the form
            ``<folder>/<observation_name>/<measurement_name>.jpg``.
        """
        return str(
            PurePosixPath(WebAssetFolderName.for_asset_kind(self.kind).value)
            / self.observation_name
            / f"{self.measurement_name}.jpg"
        )
