"""Tests for core models."""

import datetime
from pathlib import Path

import numpy as np
import pytest

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    FlatField,
    MaxDeltaPolicy,
    Measurement,
    MeasurementMetadata,
    StokesParameters,
    _decode_info,
    _parse_zimpol_datetime,
)
from tests.unit.utils import make_dat_array_info


class TestCalibrationResult:
    @staticmethod
    def make_calibration_result(
        pixel_scale: float, wavelength_offset: float
    ) -> CalibrationResult:
        return CalibrationResult(
            pixel_scale=pixel_scale,
            wavelength_offset=wavelength_offset,
            pixel_scale_error=-1,
            wavelength_offset_error=-1,
            reference_file="",
        )

    @pytest.mark.parametrize(
        "pixel_scale,wavelength_offset,pixel,expected",
        [
            (10, 3405, 0, 3405),  # pixel 0 should give wavelength_offset
            (10, 3405, 1, 3415),  # pixel 1 should give wavelength_offset + pixel_scale
            (
                10,
                3405,
                -1,
                3395,
            ),  # pixel -1 should give wavelength_offset - pixel_scale
            (
                10,
                3405,
                4,
                3445,
            ),  # pixel 4 should give wavelength_offset + 4 * pixel_scale
        ],
    )
    def test_pixel_to_wavelength(
        self,
        pixel_scale: float,
        wavelength_offset: float,
        pixel: float,
        expected: float,
    ):
        calibration_result = self.make_calibration_result(
            pixel_scale, wavelength_offset
        )
        wavelength = calibration_result.pixel_to_wavelength(pixel)
        assert wavelength == expected

    @pytest.mark.parametrize(
        "pixel_scale,wavelength_offset,wavelength,expected",
        [
            (10, 3405, 3405, 0),  # wavelength equal to offset should give pixel 0
            (10, 3405, 3415, 1),  # wavelength 10 above offset should give pixel 1
            (10, 3405, 3395, -1),  # wavelength 10 below offset should give pixel -1
            (10, 3405, 3445, 4),  # wavelength 40 above offset should give pixel 4
        ],
    )
    def test_wavelength_to_pixel(
        self,
        pixel_scale: float,
        wavelength_offset: float,
        wavelength: float,
        expected: float,
    ):
        calibration_result = self.make_calibration_result(
            pixel_scale, wavelength_offset
        )
        pixel = calibration_result.wavelength_to_pixel(wavelength)
        assert pixel == expected


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


class TestDecodeInfo:
    def test_basic_decoding(self):
        info = make_dat_array_info({"key1": "val1", "key2": "val2"})
        result = _decode_info(info)
        assert result == {"key1": "val1", "key2": "val2"}

    def test_empty_array(self):
        info = np.array([], dtype=object).reshape(0, 2)
        result = _decode_info(info)
        assert result == {}


class TestParseZimpolDatetime:
    def test_with_offset(self):
        dt_result = _parse_zimpol_datetime("2024-07-13T10:22:00+01")
        assert dt_result.year == 2024
        assert dt_result.month == 7
        assert dt_result.hour == 9  # 10 - 1 hour offset
        assert dt_result.tzinfo == datetime.timezone.utc

    def test_without_offset(self):
        dt_result = _parse_zimpol_datetime("2024-07-13T10:22:00")
        assert dt_result.hour == 10
        assert dt_result.tzinfo == datetime.timezone.utc


class TestMeasurementMetadata:
    def test_from_info_array(self, sample_measurement_metadata: MeasurementMetadata):

        # -- measurement core --
        assert (
            sample_measurement_metadata.file
            == "/global/data1/zimpol/2025/251111/raw/5886_m13.z3bd"
        )
        assert sample_measurement_metadata.telescope_name == "Gregory IRSOL"
        assert sample_measurement_metadata.instrument_post_focus == "Spectrograph"
        assert sample_measurement_metadata.instrument == "ZIMPOL3"
        assert sample_measurement_metadata.modulator_type == "PEM"
        assert sample_measurement_metadata.project == "flare5884"
        assert sample_measurement_metadata.observer == "afb"
        assert sample_measurement_metadata.wavelength == 5886
        assert sample_measurement_metadata.name == "5886_m13"
        assert sample_measurement_metadata.datetime_start == _parse_zimpol_datetime(
            "2025-11-11T09:43:16+01:00"
        )
        assert sample_measurement_metadata.datetime_end == _parse_zimpol_datetime(
            "2025-11-11T09:44:42+01:00"
        )
        assert sample_measurement_metadata.type == "SCIENCE"
        assert sample_measurement_metadata.id == 1762850596
        assert sample_measurement_metadata.sequence_length == 2
        assert sample_measurement_metadata.sub_sequence_length == 4
        assert (
            sample_measurement_metadata.sub_sequence_name == "TCU0Q TCU0U TCU1Q TCU1U"
        )
        assert sample_measurement_metadata.stokes_vector == "IQUV"
        assert sample_measurement_metadata.integration_time == 0.1
        assert sample_measurement_metadata.images == [16, 16, 16, 16]
        assert sample_measurement_metadata.image_type == "Spectrum"
        assert sample_measurement_metadata.image_type_x == "spectral"
        assert sample_measurement_metadata.image_type_y == "spatial"
        assert sample_measurement_metadata.guiding_status == 2
        assert sample_measurement_metadata.pig_intensity == 115
        assert sample_measurement_metadata.solar_disc_coordinates == "344.5 447.0"
        assert sample_measurement_metadata.solar_p0 == 22.3
        assert sample_measurement_metadata.limbguider_status == 0
        assert sample_measurement_metadata.polcomp_status == 0

        # -- camera sub-model --
        assert sample_measurement_metadata.camera.identity == "CAM2"
        assert sample_measurement_metadata.camera.ccd == "03262-21-09"
        assert sample_measurement_metadata.camera.temperature == -15.02
        assert sample_measurement_metadata.camera.position == "0 560 0 1240"

        # -- spectrograph sub-model --
        assert sample_measurement_metadata.spectrograph.alpha == 27.73
        assert sample_measurement_metadata.spectrograph.grtwl == 5886.139
        assert sample_measurement_metadata.spectrograph.order == 5
        assert sample_measurement_metadata.spectrograph.slit == 0.06

        # -- derotator sub-model --
        assert sample_measurement_metadata.derotator.coordinate_system == 1
        assert sample_measurement_metadata.derotator.position_angle == 45.0
        assert sample_measurement_metadata.derotator.offset == 0.0

        # -- TCU sub-model --
        assert sample_measurement_metadata.tcu.mode == 1
        assert sample_measurement_metadata.tcu.retarder_name == "HWP_550"
        assert (
            sample_measurement_metadata.tcu.retarder_wl_parameter
            == "0.50 549.99 12766003. 1"
        )
        assert (
            sample_measurement_metadata.tcu.positions
            == "0.0 45.0 90.0 135.0 22.5 67.5 112.5 157.5"
        )

        # -- reduction sub-model --
        assert (
            sample_measurement_metadata.reduction.software
            == "Z3reduce.pro (v06.05.2020)"
        )
        assert sample_measurement_metadata.reduction.status is True
        assert sample_measurement_metadata.reduction.file == "5886_m13.z3bd"
        assert sample_measurement_metadata.reduction.number_of_files == 1
        assert (
            sample_measurement_metadata.reduction.file_dc_used
            == "/global/data1/zimpol/2025/251111/reduced/dark00100_m4.sav"
        )
        assert sample_measurement_metadata.reduction.dcfit == "poly_fit 7"
        assert (
            sample_measurement_metadata.reduction.demodulation_matrix
            == "1  1  1  1  0  0  0  0 -1  1  1 -1  1  1 -1 -1"
        )
        assert sample_measurement_metadata.reduction.order_of_rows == [0, 1, 2, 3]
        assert sample_measurement_metadata.reduction.mode == "two phase subtraction"
        assert sample_measurement_metadata.reduction.tcu_method == "0"
        assert sample_measurement_metadata.reduction.outfname == "5886_m13.dat"

        # -- calibration sub-model --
        assert (
            sample_measurement_metadata.calibration.software
            == "Z3calibrate.pro (24.09.2024)"
        )
        assert (
            sample_measurement_metadata.calibration.file
            == "/global/data1/zimpol/2025/251111/raw/cal5886_m2.z3bd"
        )
        assert sample_measurement_metadata.calibration.status is True
        assert (
            sample_measurement_metadata.calibration.description == "r12, r22, r32, r33"
        )

        # -- top-level flags --
        assert sample_measurement_metadata.flatfield_status is False
        assert sample_measurement_metadata.global_noise is not None
        assert sample_measurement_metadata.global_mean is not None

        # -- computed properties --
        assert sample_measurement_metadata.solar_x == 344.5
        assert sample_measurement_metadata.solar_y == 447.0


class TestMaxDeltaPolicy:
    def test_default_policy(self):
        policy = MaxDeltaPolicy()
        delta = policy.get_max_delta(wavelength=6302)
        assert delta == datetime.timedelta(hours=2)

    def test_custom_default(self):
        policy = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=4))
        delta = policy.get_max_delta(wavelength=6302)
        assert delta == datetime.timedelta(hours=4)

    def test_subclass_override(self):
        class CustomPolicy(MaxDeltaPolicy):
            def get_max_delta(self, wavelength, instrument="", telescope=""):
                if wavelength < 5000:
                    return datetime.timedelta(hours=1)
                return self.default_max_delta

        policy = CustomPolicy()
        assert policy.get_max_delta(wavelength=4078) == datetime.timedelta(hours=1)
        assert policy.get_max_delta(wavelength=6302) == datetime.timedelta(hours=2)
