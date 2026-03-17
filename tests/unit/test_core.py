"""Tests for core domain models."""

import datetime
from pathlib import Path

import pytest

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    FlatField,
    Measurement,
    MeasurementMetadata,
    StokesParameters,
)


class TestStokesParameters:
    def test_creation(self, sample_stokes: StokesParameters):
        assert sample_stokes.i.shape == (50, 200)
        assert sample_stokes.q.shape == (50, 200)

    def test_unpacking(self, sample_stokes: StokesParameters):
        i, q, u, v = sample_stokes
        assert i.shape == (50, 200)


class TestMeasurement:
    def test_properties(
        self,
        sample_measurement_metadata: MeasurementMetadata,
        sample_stokes: StokesParameters,
    ):
        m = Measurement(
            source_path=Path("/data/5886_m13.dat"),
            metadata=sample_measurement_metadata,
            stokes=sample_stokes,
        )
        assert m.wavelength == 5886
        assert m.name == "5886_m13"
        assert isinstance(m.timestamp, datetime.datetime)


class TestFlatField:
    def test_properties(
        self,
        sample_measurement_metadata: MeasurementMetadata,
        sample_stokes: StokesParameters,
    ):
        ff = FlatField(
            source_path=Path("/data/ff5886_m13.dat"),
            metadata=sample_measurement_metadata,
            stokes=sample_stokes,
        )
        assert ff.wavelength == 5886
        assert isinstance(ff.timestamp, datetime.datetime)


class TestCalibrationResult:
    def test_pixel_to_wavelength(self):
        cal = CalibrationResult(
            pixel_scale=0.01,
            wavelength_offset=6300.0,
            pixel_scale_error=0.001,
            wavelength_offset_error=0.1,
            reference_file="ref.npy",
        )
        assert cal.pixel_to_wavelength(100) == pytest.approx(6301.0)

    def test_wavelength_to_pixel(self):
        cal = CalibrationResult(
            pixel_scale=0.01,
            wavelength_offset=6300.0,
            pixel_scale_error=0.001,
            wavelength_offset_error=0.1,
            reference_file="ref.npy",
        )
        assert cal.wavelength_to_pixel(6301.0) == pytest.approx(100.0)
