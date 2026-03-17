import numpy as np
import pytest

from irsol_data_pipeline.core.models import (
    MeasurementMetadata,
    StokesParameters,
)

from .utils import make_dat_array_info

_FULL_DAT_INFO_DICT: dict[str, str] = {
    "reduction.software": "Z3reduce.pro (v06.05.2020)",
    "reduction.status": "yes",
    "measurement.file": "/global/data1/zimpol/2025/251111/raw/5886_m13.z3bd",
    "measurement.telescope name": "Gregory IRSOL",
    "measurement.instrument.post-focus": "Spectrograph",
    "measurement.instrument": "ZIMPOL3",
    "measurement.modulator type": "PEM",
    "measurement.project": "flare5884",
    "measurement.observer": "afb",
    "measurement.wavelength": "5886",
    "measurement.name": "5886_m13",
    "measurement.datetime": "2025-11-11T09:43:16+01:00",
    "measurement.datetime.end": "2025-11-11T09:44:42+01:00",
    "measurement.type": "SCIENCE",
    "measurement.id": "1762850596",
    "measurement.sequence.length": "2",
    "measurement.sub-sequence.length": "4",
    "measurement.sub-sequence.name": "TCU0Q TCU0U TCU1Q TCU1U ",
    "measurement.stokes vector": "IQUV",
    "measurement.integration time": "0.1",
    "measurement.images": "16 16 16 16 ",
    "measurement.camera.identity": "CAM2",
    "measurement.camera.CCD": "03262-21-09",
    "measurement.camera.temperature": "-15.02",
    "measurement.camera.position": "0 560 0 1240",
    "measurement.image.type": "Spectrum",
    "measurement.image.type_x": "spectral",
    "measurement.image.type_y": "spatial",
    "measurement.guiding.status": "2",
    "measurement.pig.intensity": "115",
    "measurement.solar_disc.coordinates": "344.5 447.0",
    "measurement.derotator.coordinate_system": "1",
    "measurement.derotator.position_angle": "45.0",
    "measurement.sun.p0": "22.3",
    "measurement.limbguider.status": "0",
    "measurement.polcomp.status": "0",
    "measurement.spectrograph.alpha": "27.73",
    "measurement.spectrograph.grtwl": "5886.139",
    "measurement.spectrograph.order": "5",
    "measurement.spectrograph.slit": "0.06",
    "measurement.TCU.mode": "1",
    "measurement.TCU.retarder.name": "HWP_550",
    "measurement.TCU.retarder.wl_parameter": "0.50 549.99 12766003. 1",
    "measurement.TCU.positions": "0.0 45.0 90.0 135.0 22.5 67.5 112.5 157.5 ",
    "measurement.derotator.offset": "0",
    "reduction.file": "5886_m13.z3bd",
    "reduction.number of files": "1",
    "reduction.file.dc.used": "/global/data1/zimpol/2025/251111/reduced/dark00100_m4.sav",
    "reduction.dcfit": "poly_fit 7",
    "reduction.demodulation matrix": "  1  1  1  1  0  0  0  0 -1  1  1 -1  1  1 -1 -1",
    "reduction.order of rows": " 0 1 2 3",
    "reduction.mode": "two phase subtraction",
    "calibration.software": "Z3calibrate.pro (24.09.2024)",
    "calibration.file": "/global/data1/zimpol/2025/251111/raw/cal5886_m2.z3bd",
    "calibration.status": "yes",
    "calibration.description": "r12, r22, r32, r33",
    "reduction.TCU.method": "       0",
    "flatfield.status": "no",
    "global.noise": "440.579  0.000601946  0.000599725  0.000571274",
    "reduction.pixels replaced": "3294           3           1          30",
    "global.mean": "59005.9 -2.59019e-05 -2.27992e-05   0.00602865",
    "reduction.outfname": "5886_m13.dat",
}


@pytest.fixture
def sample_dat_info_dict() -> dict[str, str]:
    return _FULL_DAT_INFO_DICT


@pytest.fixture
def sample_dat_info_array(sample_dat_info_dict: dict[str, str]) -> np.ndarray:
    return make_dat_array_info(sample_dat_info_dict)


@pytest.fixture
def sample_measurement_metadata(
    sample_dat_info_array: np.ndarray,
) -> MeasurementMetadata:
    return MeasurementMetadata.from_info_array(sample_dat_info_array)


@pytest.fixture
def sample_stokes() -> StokesParameters:
    return StokesParameters(
        i=np.ones((50, 200)),
        q=np.zeros((50, 200)),
        u=np.zeros((50, 200)),
        v=np.zeros((50, 200)),
    )
