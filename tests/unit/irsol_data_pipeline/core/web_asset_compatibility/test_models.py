"""Unit tests for the web_asset_compatibility package."""

from __future__ import annotations

from pathlib import Path

import pytest

from irsol_data_pipeline.core.web_asset_compatibility.models import (
    WebAssetFolderName,
    WebAssetKind,
    WebAssetSource,
)


class TestWebAssetKind:
    def test_enum_values(self) -> None:
        assert WebAssetKind.QUICK_LOOK.value == "quicklook"
        assert WebAssetKind.CONTEXT.value == "context"

    def test_two_members(self) -> None:
        assert len(list(WebAssetKind)) == 2


class TestWebAssetSource:
    def _make(self, **kwargs: object) -> WebAssetSource:
        defaults: dict[str, object] = {
            "kind": WebAssetKind.QUICK_LOOK,
            "observation_name": "250101",
            "measurement_name": "5876_m01",
            "source_path": Path("/tmp/src.png"),
        }
        defaults.update(kwargs)
        return WebAssetSource(**defaults)  # type: ignore[arg-type]

    def test_construction(self) -> None:
        src = self._make()
        assert src.kind is WebAssetKind.QUICK_LOOK
        assert src.observation_name == "250101"
        assert src.measurement_name == "5876_m01"

    def test_frozen(self) -> None:
        src = self._make()
        with pytest.raises(Exception):
            src.measurement_name = "changed"  # type: ignore[misc]

    @pytest.mark.parametrize("kind", list(WebAssetKind))
    def test_accepts_both_kinds(self, kind: WebAssetKind) -> None:
        src = self._make(kind=kind)
        assert src.kind is kind

    @pytest.mark.parametrize(
        "kind,expected_folder",
        [
            (WebAssetKind.QUICK_LOOK, WebAssetFolderName.QUICK_LOOK.value),
            (WebAssetKind.CONTEXT, WebAssetFolderName.CONTEXT.value),
        ],
    )
    def test_remote_target_path_uses_correct_folder(
        self, kind: WebAssetKind, expected_folder: str
    ) -> None:
        src = self._make(
            kind=kind, observation_name="250101", measurement_name="5876_m01"
        )
        assert src.remote_target_path.startswith(expected_folder + "/")

    def test_remote_target_path_contains_observation_and_measurement(self) -> None:
        src = self._make(
            kind=WebAssetKind.QUICK_LOOK,
            observation_name="250101",
            measurement_name="5876_m01",
        )
        assert "250101" in src.remote_target_path
        assert "5876_m01.jpg" in src.remote_target_path

    def test_remote_target_path_is_posix(self) -> None:
        src = self._make(observation_name="250101", measurement_name="5876_m01")
        # Should use forward slashes, not backslashes
        assert "\\" not in src.remote_target_path
