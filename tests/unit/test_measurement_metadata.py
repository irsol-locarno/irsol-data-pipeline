"""Tests for core metadata abstraction."""

import datetime

import numpy as np

from irsol_data_pipeline.core.models import (
    MeasurementMetadata,
    _decode_info,
    _parse_zimpol_datetime,
)

from .utils import make_dat_array_info


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
