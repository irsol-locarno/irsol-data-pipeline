from __future__ import annotations

import datetime
from pathlib import Path

import pytest

from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    SolarOrientationInfo,
    StokesParameters,
)
from irsol_data_pipeline.core.solar_orientation import compute_solar_orientation
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.io import fits as fits_io
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits


class TestFitsImporter:
    @pytest.fixture(scope="module")
    def fits_path(self, fixture_dir: Path) -> Path:
        return fixture_dir / "5886_m13.fits"

    def test_read_valid_fits(self, fits_path: Path):
        fits_data = fits_io.read(fits_path)

        assert isinstance(fits_data.stokes, StokesParameters)
        assert isinstance(fits_data.calibration, (CalibrationResult, type(None)))


class TestFitsMeasurementMetadataRoundtrip:
    """Tests that MeasurementMetadata survives a .dat → FITS → reload round-
    trip."""

    @pytest.fixture(scope="class")
    def dat_path(self, fixture_dir: Path) -> Path:
        return fixture_dir / "5886_m14.dat"

    @pytest.fixture(scope="class")
    def dat_metadata(self, dat_path: Path) -> MeasurementMetadata:
        """Load MeasurementMetadata from the .dat fixture file."""
        _stokes, info = dat_io.read(dat_path)
        return MeasurementMetadata.from_info_array(info)

    @pytest.fixture(scope="class")
    def dat_stokes(self, dat_path: Path) -> StokesParameters:
        """Load Stokes parameters from the .dat fixture file."""
        stokes, _info = dat_io.read(dat_path)
        return stokes

    @pytest.fixture(scope="class")
    def dat_solar_orientation(
        self, dat_metadata: MeasurementMetadata
    ) -> SolarOrientationInfo:
        """Compute SolarOrientationInfo from the dat metadata."""
        return compute_solar_orientation(dat_metadata)

    @pytest.fixture(scope="class")
    def fits_path(
        self,
        tmp_path_factory,
        dat_stokes: StokesParameters,
        dat_metadata: MeasurementMetadata,
        dat_solar_orientation: SolarOrientationInfo,
    ) -> Path:
        """Write a FITS file from dat data and return the path."""
        tmp_path = tmp_path_factory.mktemp("fits_roundtrip")
        path = tmp_path / "roundtrip.fits"
        write_stokes_fits(
            output_path=path,
            stokes=dat_stokes,
            info=dat_metadata,
            calibration=None,
            solar_orientation=dat_solar_orientation,
        )
        return path

    @pytest.fixture(scope="class")
    def fits_metadata(self, fits_path: Path) -> MeasurementMetadata:
        """Reload MeasurementMetadata from the written FITS file."""
        loaded = fits_io.read(fits_path)
        assert loaded.metadata is not None, (
            "Expected metadata to be extracted from FITS"
        )
        return loaded.metadata

    @pytest.fixture(scope="class")
    def fits_solar_orientation(self, fits_path: Path) -> SolarOrientationInfo:
        """Reload SolarOrientationInfo from the written FITS file."""
        loaded = fits_io.read(fits_path)
        assert loaded.solar_orientation is not None, (
            "Expected solar_orientation to be extracted from FITS"
        )
        return loaded.solar_orientation

    # ------------------------------------------------------------------
    # Core identification fields
    # ------------------------------------------------------------------

    def test_telescope_name(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.telescope_name == dat_metadata.telescope_name

    def test_instrument(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.instrument == dat_metadata.instrument

    def test_type(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.type == dat_metadata.type

    def test_id(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.id == dat_metadata.id

    def test_name(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.name == dat_metadata.name

    def test_wavelength(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.wavelength == dat_metadata.wavelength

    def test_datetime_start(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        expected = dat_metadata.datetime_start.astimezone(datetime.timezone.utc)
        actual = fits_metadata.datetime_start.astimezone(datetime.timezone.utc)
        assert actual == expected

    def test_datetime_end(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.datetime_end is None:
            assert fits_metadata.datetime_end is None
        else:
            expected = dat_metadata.datetime_end.astimezone(datetime.timezone.utc)
            actual = fits_metadata.datetime_end
            assert actual is not None
            assert actual.astimezone(datetime.timezone.utc) == expected

    def test_observer(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.observer == dat_metadata.observer

    def test_project(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.project == dat_metadata.project

    def test_integration_time(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.integration_time == pytest.approx(
            dat_metadata.integration_time
        )

    # ------------------------------------------------------------------
    # Extended top-level fields
    # ------------------------------------------------------------------

    def test_instrument_post_focus(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.instrument_post_focus == dat_metadata.instrument_post_focus

    def test_modulator_type(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.modulator_type == dat_metadata.modulator_type

    def test_sequence_length(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.sequence_length == dat_metadata.sequence_length

    def test_sub_sequence_length(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.sub_sequence_length == dat_metadata.sub_sequence_length

    def test_sub_sequence_name(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.sub_sequence_name is not None:
            assert (
                fits_metadata.sub_sequence_name
                == dat_metadata.sub_sequence_name.strip()
                or fits_metadata.sub_sequence_name == dat_metadata.sub_sequence_name
            )

    def test_stokes_vector(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.stokes_vector == dat_metadata.stokes_vector

    def test_images(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.images == dat_metadata.images

    def test_image_type(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.image_type == dat_metadata.image_type

    def test_image_type_x(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.image_type_x == dat_metadata.image_type_x

    def test_image_type_y(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.image_type_y == dat_metadata.image_type_y

    def test_guiding_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.guiding_status == dat_metadata.guiding_status

    def test_pig_intensity(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.pig_intensity == dat_metadata.pig_intensity

    def test_solar_disc_coordinates(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.solar_disc_coordinates == dat_metadata.solar_disc_coordinates
        )

    def test_limbguider_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.limbguider_status == dat_metadata.limbguider_status

    def test_polcomp_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.polcomp_status == dat_metadata.polcomp_status

    def test_flatfield_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.flatfield_status == dat_metadata.flatfield_status

    def test_global_noise(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.global_noise == dat_metadata.global_noise

    def test_global_mean(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.global_mean == dat_metadata.global_mean

    # ------------------------------------------------------------------
    # Camera sub-model
    # ------------------------------------------------------------------

    def test_camera_identity(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.camera.identity == dat_metadata.camera.identity

    def test_camera_ccd(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.camera.ccd == dat_metadata.camera.ccd

    def test_camera_temperature(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.camera.temperature == pytest.approx(
            dat_metadata.camera.temperature
        )

    def test_camera_position(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.camera.position == dat_metadata.camera.position

    # ------------------------------------------------------------------
    # Spectrograph sub-model
    # ------------------------------------------------------------------

    def test_spectrograph_alpha(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.spectrograph.alpha is not None:
            assert fits_metadata.spectrograph.alpha == pytest.approx(
                dat_metadata.spectrograph.alpha
            )

    def test_spectrograph_grtwl(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.spectrograph.grtwl is not None:
            assert fits_metadata.spectrograph.grtwl == pytest.approx(
                dat_metadata.spectrograph.grtwl
            )

    def test_spectrograph_order(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.spectrograph.order == dat_metadata.spectrograph.order

    def test_spectrograph_slit(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.spectrograph.slit is not None:
            assert fits_metadata.spectrograph.slit == pytest.approx(
                dat_metadata.spectrograph.slit
            )

    # ------------------------------------------------------------------
    # Derotator sub-model
    # ------------------------------------------------------------------

    def test_derotator_coordinate_system(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.derotator.coordinate_system
            == dat_metadata.derotator.coordinate_system
        )

    def test_derotator_position_angle(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.derotator.position_angle is not None:
            assert fits_metadata.derotator.position_angle == pytest.approx(
                dat_metadata.derotator.position_angle
            )

    def test_derotator_offset(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.derotator.offset is not None:
            assert fits_metadata.derotator.offset == pytest.approx(
                dat_metadata.derotator.offset
            )

    # ------------------------------------------------------------------
    # TCU sub-model
    # ------------------------------------------------------------------

    def test_tcu_mode(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.tcu.mode == dat_metadata.tcu.mode

    def test_tcu_retarder_name(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.tcu.retarder_name == dat_metadata.tcu.retarder_name

    def test_tcu_retarder_wl_parameter(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.tcu.retarder_wl_parameter
            == dat_metadata.tcu.retarder_wl_parameter
        )

    def test_tcu_positions(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.tcu.positions is not None:
            assert (
                fits_metadata.tcu.positions == dat_metadata.tcu.positions.strip()
                or fits_metadata.tcu.positions == dat_metadata.tcu.positions
            )

    # ------------------------------------------------------------------
    # Reduction sub-model
    # ------------------------------------------------------------------

    def test_reduction_software(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.software == dat_metadata.reduction.software

    def test_reduction_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.status == dat_metadata.reduction.status

    def test_reduction_file(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.file == dat_metadata.reduction.file

    def test_reduction_number_of_files(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.reduction.number_of_files
            == dat_metadata.reduction.number_of_files
        )

    def test_reduction_file_dc_used(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.reduction.file_dc_used == dat_metadata.reduction.file_dc_used
        )

    def test_reduction_dcfit(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.dcfit == dat_metadata.reduction.dcfit

    def test_reduction_demodulation_matrix(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.reduction.demodulation_matrix
            == dat_metadata.reduction.demodulation_matrix
        )

    def test_reduction_order_of_rows(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.reduction.order_of_rows
            == dat_metadata.reduction.order_of_rows
        )

    def test_reduction_mode(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.mode == dat_metadata.reduction.mode

    def test_reduction_tcu_method(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.reduction.tcu_method is not None:
            # Leading/trailing whitespace may be stripped by FITS round-trip
            assert (
                fits_metadata.reduction.tcu_method
                == dat_metadata.reduction.tcu_method.strip()
                or fits_metadata.reduction.tcu_method
                == dat_metadata.reduction.tcu_method
            )

    def test_reduction_pixels_replaced(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.reduction.pixels_replaced is not None:
            assert (
                fits_metadata.reduction.pixels_replaced
                == dat_metadata.reduction.pixels_replaced.strip()
                or fits_metadata.reduction.pixels_replaced
                == dat_metadata.reduction.pixels_replaced
            )

    def test_reduction_outfname(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.reduction.outfname == dat_metadata.reduction.outfname

    # ------------------------------------------------------------------
    # CalibrationInfo sub-model
    # ------------------------------------------------------------------

    def test_calibration_software(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.calibration.software == dat_metadata.calibration.software

    def test_calibration_file(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.calibration.file == dat_metadata.calibration.file

    def test_calibration_status(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert fits_metadata.calibration.status == dat_metadata.calibration.status

    def test_calibration_description(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        assert (
            fits_metadata.calibration.description
            == dat_metadata.calibration.description
        )

    # ------------------------------------------------------------------
    # Solar P0 and orientation
    # ------------------------------------------------------------------

    def test_solar_p0(
        self, dat_metadata: MeasurementMetadata, fits_metadata: MeasurementMetadata
    ):
        if dat_metadata.solar_p0 is not None:
            assert fits_metadata.solar_p0 == pytest.approx(dat_metadata.solar_p0)

    def test_solar_orientation_slit_angle(
        self,
        dat_solar_orientation: SolarOrientationInfo,
        fits_solar_orientation: SolarOrientationInfo,
    ):
        assert fits_solar_orientation.slit_angle_solar_deg == pytest.approx(
            dat_solar_orientation.slit_angle_solar_deg
        )

    def test_solar_orientation_sun_p0(
        self,
        dat_solar_orientation: SolarOrientationInfo,
        fits_solar_orientation: SolarOrientationInfo,
    ):
        # FITS header float precision is limited, so allow a larger tolerance
        assert fits_solar_orientation.sun_p0_deg == pytest.approx(
            dat_solar_orientation.sun_p0_deg, abs=0.05
        )
