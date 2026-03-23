"""Tests for the flat-field cache."""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np

from irsol_data_pipeline.core.models import FlatFieldCorrection
from irsol_data_pipeline.pipeline.flatfield_cache import FlatFieldCache


def _make_correction(
    wavelength: int,
    timestamp: datetime.datetime,
    path_name: str = "ff_test.dat",
) -> FlatFieldCorrection:
    return FlatFieldCorrection(
        source_flatfield_path=Path(f"/data/{path_name}"),
        dust_flat=np.ones((50, 200)),
        offset_map=None,
        desmiled=np.ones((50, 200)),
        timestamp=timestamp,
        wavelength=wavelength,
    )


class TestFlatFieldCache:
    def test_add_and_find(self):
        cache = FlatFieldCache()
        ts = datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc)
        corr = _make_correction(6302, ts)
        cache.add_correction(corr)

        measurement_ts = datetime.datetime(
            2024, 7, 13, 10, 30, tzinfo=datetime.timezone.utc
        )
        result = cache.find_best_correction(6302, measurement_ts)
        assert result is not None
        assert result.wavelength == 6302

    def test_finds_closest(self):
        cache = FlatFieldCache()
        ts1 = datetime.datetime(2024, 7, 13, 8, 0, tzinfo=datetime.timezone.utc)
        ts2 = datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts1, "ff1.dat"))
        cache.add_correction(_make_correction(6302, ts2, "ff2.dat"))

        # Closer to ts2
        measurement_ts = datetime.datetime(
            2024, 7, 13, 9, 45, tzinfo=datetime.timezone.utc
        )
        result = cache.find_best_correction(6302, measurement_ts)
        assert result is not None
        assert result.source_flatfield_path.name == "ff2.dat"

    def test_respects_max_delta(self):
        cache = FlatFieldCache(max_delta=datetime.timedelta(hours=1))
        ts = datetime.datetime(2024, 7, 13, 8, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts))

        # Too far away
        measurement_ts = datetime.datetime(
            2024, 7, 13, 12, 0, tzinfo=datetime.timezone.utc
        )
        result = cache.find_best_correction(6302, measurement_ts)
        assert result is None

    def test_no_match_different_wavelength(self):
        cache = FlatFieldCache()
        ts = datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts))

        result = cache.find_best_correction(4078, ts)
        assert result is None

    def test_wavelengths_property(self):
        cache = FlatFieldCache()
        ts = datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts))
        cache.add_correction(_make_correction(4078, ts))

        assert cache.wavelengths == [4078, 6302]

    def test_len(self):
        cache = FlatFieldCache()
        assert len(cache) == 0
        ts = datetime.datetime(2024, 7, 13, 10, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts))
        assert len(cache) == 1

    def test_custom_max_delta_per_query(self):
        cache = FlatFieldCache(max_delta=datetime.timedelta(hours=2))
        ts = datetime.datetime(2024, 7, 13, 8, 0, tzinfo=datetime.timezone.utc)
        cache.add_correction(_make_correction(6302, ts))

        measurement_ts = datetime.datetime(
            2024, 7, 13, 9, 30, tzinfo=datetime.timezone.utc
        )

        # Should find with default 2h
        assert cache.find_best_correction(6302, measurement_ts) is not None

        # Should not find with 30min override
        assert (
            cache.find_best_correction(
                6302, measurement_ts, max_delta=datetime.timedelta(minutes=30)
            )
            is None
        )
