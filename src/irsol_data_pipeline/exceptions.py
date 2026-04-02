import datetime
from pathlib import Path

from irsol_data_pipeline.core.models import Measurement


class IrsolDataPipelineException(Exception):
    """Base exception for the irsol data pipeline."""


class InvalidMeasurementDataException(IrsolDataPipelineException, ValueError):
    """Exception raised when measurement data is invalid or cannot be
    processed."""


class SmileCorrectionException(IrsolDataPipelineException, RuntimeError):
    """Exception raised when smile correction fails for a measurement."""


class AutocalibrationReferenceFilesNotFound(
    IrsolDataPipelineException,
    FileNotFoundError,
):
    """Raised when no autocalibration reference files are found for a
    measurement."""

    def __init__(self, provided_path: Path):
        self.provided_path = provided_path
        super().__init__(f"No reference data files found in {provided_path}")


class FlatFieldAssociationNotFoundException(IrsolDataPipelineException):
    """Exception raised when no flat field has been found to be associated with
    a measurement."""

    def __init__(
        self,
        message: str = "",
        *,
        measurement: Measurement | None = None,
        max_delta: datetime.timedelta | None = None,
        target_angle: float | None = None,
    ):
        self.measurement = measurement
        self.max_delta = max_delta
        self.target_angle = target_angle

        super().__init__(
            message,
            f"No flat-field within {max_delta or '<unspecified>'} for wavelength "
            f"{measurement.wavelength} at {measurement.timestamp}"
            if measurement
            else "<unspecified>",
            f"and target angle {target_angle}"
            if target_angle is not None
            else "<unspecified>",
        )


class DatError(IrsolDataPipelineException, RuntimeError):
    """Exception raised when an error occurs during Dat file writing or
    reading."""


class DatImportError(DatError):
    """Exception raised when an error occurs during Dat file reading."""


class FitsError(IrsolDataPipelineException, RuntimeError):
    """Exception raised when an error occurs during FITS file writing or
    reading."""


class FitsImportError(FitsError):
    """Exception raised when an error occurs during FITS file reading."""


class FitsExportError(FitsError):
    """Exception raised when an error occurs during FITS file writing."""


class FlatfieldCorrectionError(IrsolDataPipelineException, RuntimeError):
    """Exception raised when an error occurs during flat-fild correction file
    reading/writing."""


class FlatfieldCorrectionImportError(FlatfieldCorrectionError):
    """Exception raised when an error occurs during flat-fild correction file
    reading."""


class FlatfieldCorrectionExportError(FlatfieldCorrectionError):
    """Exception raised when an error occurs during flat-field correction file
    writing."""


class SlitImageGenerationError(IrsolDataPipelineException, RuntimeError):
    """Exception raised when slit image generation fails."""


class DatasetRootNotConfiguredError(IrsolDataPipelineException, ValueError):
    """Raised when no dataset root is provided and no default is configured."""

    def __init__(self, variable_name: str):
        self.variable_name = variable_name
        super().__init__(
            "No dataset root path provided and no default Prefect Variable is set "
            f"for '{variable_name}'.",
        )


class WebAssetUploadError(IrsolDataPipelineException, RuntimeError):
    """Raised when web-asset compatibility upload fails."""
