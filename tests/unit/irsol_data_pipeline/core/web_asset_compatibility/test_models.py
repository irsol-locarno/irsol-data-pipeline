"""Unit tests for the web_asset_compatibility package."""

from __future__ import annotations

from pathlib import Path

import pytest

from irsol_data_pipeline.core.web_asset_compatibility.models import (
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
            "target_path": Path("/tmp/dst.jpg"),
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
