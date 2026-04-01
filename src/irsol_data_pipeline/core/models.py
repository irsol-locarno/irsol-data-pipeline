"""Domain model."""

from __future__ import annotations

import datetime
import re
from pathlib import Path

import numpy as np
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator

from irsol_data_pipeline.core.config import DEFAULT_MAX_DELTA


class CalibrationResult(BaseModel):
    """Result of wavelength auto-calibration.

    The calibration maps pixel positions to wavelengths using a linear model:
        wavelength = pixel_scale * pixel + wavelength_offset
    """

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    pixel_scale: float  # a1: angstrom per pixel
    wavelength_offset: float  # a0: wavelength at pixel 0
    pixel_scale_error: float  # 1-sigma error on a1
    wavelength_offset_error: float  # 1-sigma error on a0
    reference_file: str  # name of reference data file used
    peak_pixels: np.ndarray | None = None  # pixel positions of fitted peaks
    reference_lines: np.ndarray | None = None  # wavelengths of the reference lines

    def pixel_to_wavelength(self, pixel: float) -> float:
        """Convert a pixel position to wavelength in Angstrom."""
        return self.pixel_scale * pixel + self.wavelength_offset

    def wavelength_to_pixel(self, wavelength: float) -> float:
        """Convert a wavelength in Angstrom to pixel position."""
        return (wavelength - self.wavelength_offset) / self.pixel_scale


class FlatField(BaseModel):
    """A flat-field measurement loaded from a .dat file."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_path: Path
    metadata: MeasurementMetadata
    stokes: StokesParameters

    @property
    def wavelength(self) -> int:
        return self.metadata.wavelength

    @property
    def timestamp(self) -> datetime.datetime:
        return self.metadata.datetime_start

    @property
    def name(self) -> str:
        return self.metadata.name


class FlatFieldCorrection(BaseModel):
    """A computed flat-field correction ready to be applied.

    This stores the analysis results (dust flat map and offset map) from
    a flat-field analysis. The offset_map type depends on the correction
    backend (e.g. spectroflat OffsetMap).
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_flatfield_path: Path
    dust_flat: np.ndarray
    offset_map: object  # Backend-specific (e.g. spectroflat OffsetMap)
    desmiled: np.ndarray
    timestamp: datetime.datetime
    wavelength: int


class Measurement(BaseModel):
    """A solar observation measurement loaded from a .dat file."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source_path: Path
    metadata: MeasurementMetadata
    stokes: StokesParameters

    @property
    def wavelength(self) -> int:
        return self.metadata.wavelength

    @property
    def timestamp(self) -> datetime.datetime:
        return self.metadata.datetime_start

    @property
    def name(self) -> str:
        return self.metadata.name


class SolarOrientationInfo(BaseModel):
    """Solar orientation information computed from measurement metadata.

    Encapsulates all values needed to render a solar north indicator on
    a Stokes profile plot or other visualisations.

    The solar north direction in the plot frame (wavelength × spatial) is:

    .. code-block:: python

        import numpy as np
        angle_rad = np.radians(info.slit_angle_solar_deg)
        dx = np.cos(angle_rad)  # component along the wavelength axis
        dy = np.sin(angle_rad)  # component along the spatial axis
    """

    model_config = ConfigDict(frozen=True)

    sun_p0_deg: float
    """Position angle of the solar north pole (P0) in degrees, as returned by
    :func:`sunpy.coordinates.sun.P`."""

    slit_angle_solar_deg: float
    """Angle of the slit direction in the solar reference frame, in degrees,
    measured counter-clockwise from the solar west (positive Tx) direction.

    This follows the standard heliographic convention where 0° points west
    (positive Tx), 90° points north (positive Ty), and 180° points east.

    The solar north direction expressed in the (wavelength, spatial) plot
    frame is :math:`\\cos\\theta,\\,\\sin\\theta)` where
    :math:`\\theta` = ``slit_angle_solar_deg`` in radians.
    """

    needs_rotation: bool
    """True when the derotator coordinate system is equatorial and a P0
    rotation was applied to bring the slit angle into the solar frame."""


def _parse_yes_no(value: object) -> bool | None:
    """Parse a yes/no string into a boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"yes", "true", "1"}:
            return True
        if normalized in {"no", "false", "0"}:
            return False
    raise ValueError(f"Cannot parse boolean flag from value: {value!r}")


class ReductionInfo(BaseModel):
    """Metadata from ``reduction.*`` keys in the info array."""

    model_config = ConfigDict(frozen=True)

    software: str | None = None
    status: bool | None = None
    file: str | None = None
    number_of_files: int | None = None
    file_dc_used: str | None = None
    dcfit: str | None = None
    demodulation_matrix: str | None = None
    order_of_rows: list[int] = Field(default_factory=list)
    mode: str | None = None
    tcu_method: str | None = None
    pixels_replaced: str | None = None
    outfname: str | None = None

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: object) -> bool | None:
        return _parse_yes_no(v)

    @field_validator("order_of_rows", mode="before")
    @classmethod
    def _coerce_order_of_rows(cls, v: object) -> object:
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split() if x.strip()]
        return v


class CalibrationInfo(BaseModel):
    """Metadata from ``calibration.*`` keys in the info array."""

    model_config = ConfigDict(frozen=True)

    software: str | None = None
    file: str | None = None
    status: bool | None = None
    description: str | None = None

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: object) -> bool | None:
        return _parse_yes_no(v)


class CameraInfo(BaseModel):
    """Metadata from ``measurement.camera.*`` keys."""

    model_config = ConfigDict(frozen=True)

    identity: str | None = None
    ccd: str | None = None
    temperature: float | None = None
    position: str | None = None


class SpectrographInfo(BaseModel):
    """Metadata from ``measurement.spectrograph.*`` keys."""

    model_config = ConfigDict(frozen=True)

    alpha: float | None = None
    grtwl: float | None = None
    order: int | None = None
    slit: float | None = None


class DerotatorInfo(BaseModel):
    """Metadata from ``measurement.derotator.*`` keys."""

    model_config = ConfigDict(frozen=True)

    coordinate_system: int | None = None
    position_angle: float | None = None
    offset: float | None = None


class TCUInfo(BaseModel):
    """Metadata from ``measurement.TCU.*`` keys."""

    model_config = ConfigDict(frozen=True)

    mode: int | None = None
    retarder_name: str | None = None
    retarder_wl_parameter: str | None = None
    positions: str | None = None


class MeasurementMetadata(BaseModel):
    """Decoded metadata extracted from a ZIMPOL .dat info array.

    All fields are extracted once at construction time so that
    downstream code never touches the raw byte array.

    Sub-models group logically related keys:
    - ``reduction``: reduction pipeline metadata
    - ``calibration``: calibration metadata
    - ``camera``: camera hardware metadata
    - ``spectrograph``: spectrograph settings
    - ``derotator``: derotator settings
    - ``tcu``: TCU (calibration unit) settings
    """

    model_config = ConfigDict(
        frozen=True,
        arbitrary_types_allowed=True,
        populate_by_name=True,
    )

    # --- measurement core ---
    file: str | None = None
    telescope_name: str
    instrument_post_focus: str | None = None
    instrument: str
    modulator_type: str | None = None
    project: str = ""
    observer: str = ""
    wavelength: int
    name: str
    # Datetimes are parsed and already in UTC timezone. The original raw string is kept in _raw for reference.
    datetime_start: datetime.datetime = Field(validation_alias="datetime")
    datetime_end: datetime.datetime | None = None
    type: str
    id: int
    sequence_length: int | None = None
    sub_sequence_length: int | None = None
    sub_sequence_name: str | None = None
    stokes_vector: str | None = None
    integration_time: float | None = None
    images: list[int] = Field(default_factory=list)
    image_type: str | None = None
    image_type_x: str | None = None
    image_type_y: str | None = None
    guiding_status: int | None = None
    pig_intensity: int | None = None
    solar_disc_coordinates: str | None = None
    solar_p0: float | None = Field(default=None, validation_alias="sun_p0")
    limbguider_status: int | None = None
    polcomp_status: int | None = None

    # --- sub-models ---
    camera: CameraInfo = Field(default_factory=CameraInfo)
    spectrograph: SpectrographInfo = Field(default_factory=SpectrographInfo)
    derotator: DerotatorInfo = Field(default_factory=DerotatorInfo)
    tcu: TCUInfo = Field(default_factory=TCUInfo)
    reduction: ReductionInfo = Field(default_factory=ReductionInfo)
    calibration: CalibrationInfo = Field(default_factory=CalibrationInfo)

    # --- top-level flags outside the groups above ---
    flatfield_status: bool | None = None
    global_noise: str | None = None
    global_mean: str | None = None

    # Keep the raw decoded dict for any field we haven't explicitly modeled.
    _raw: dict[str, str] = PrivateAttr(default_factory=dict)

    @field_validator("flatfield_status", mode="before")
    @classmethod
    def _coerce_flatfield_status(cls, v: object) -> bool | None:
        return _parse_yes_no(v)

    @field_validator("datetime_start", "datetime_end", mode="before")
    @classmethod
    def _coerce_datetime(cls, v: object) -> datetime.datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime.datetime):
            return v
        if isinstance(v, str):
            return _parse_zimpol_datetime(v)
        raise ValueError(f"Cannot parse datetime from value: {v!r}")

    @field_validator("images", mode="before")
    @classmethod
    def _coerce_images(cls, v: object) -> object:
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split() if x.strip()]
        return v

    @staticmethod
    def from_info_array(info: np.ndarray) -> MeasurementMetadata:
        """Build metadata from a ZIMPOL info Nx2 byte array.

        Raw keys are routed to sub-models based on their dot-separated
        prefix (e.g. ``"measurement.camera.identity"`` populates the
        ``camera`` sub-model).  Each suffix is normalised to a Python
        field name (dots/spaces/hyphens → underscores, lowercased) and
        the resulting sub-dicts are validated via ``model_validate``.
        """
        raw = _decode_info(info)

        # Prefixes that map to sub-models (longer prefixes checked first)
        _prefix_to_submodel: list[tuple[str, str, type[BaseModel]]] = [
            ("measurement.camera.", "camera", CameraInfo),
            ("measurement.spectrograph.", "spectrograph", SpectrographInfo),
            ("measurement.derotator.", "derotator", DerotatorInfo),
            ("measurement.TCU.", "tcu", TCUInfo),
            ("reduction.", "reduction", ReductionInfo),
            ("calibration.", "calibration", CalibrationInfo),
        ]

        sub_dicts: dict[str, dict[str, str]] = {
            name: {} for _, name, _ in _prefix_to_submodel
        }
        top_level: dict[str, object] = {}

        for raw_key, raw_value in raw.items():
            value = raw_value.strip() if isinstance(raw_value, str) else raw_value
            if isinstance(value, str) and not value:
                continue

            matched = False
            for prefix, group_name, _ in _prefix_to_submodel:
                if raw_key.startswith(prefix):
                    suffix = raw_key[len(prefix) :]
                    sub_dicts[group_name][_normalize_key(suffix)] = value
                    matched = True
                    break

            if not matched:
                if raw_key.startswith("measurement."):
                    suffix = raw_key[len("measurement.") :]
                else:
                    suffix = raw_key
                top_level[_normalize_key(suffix)] = value

        # Build sub-models via model_validate
        for _prefix, group_name, model_cls in _prefix_to_submodel:
            top_level[group_name] = model_cls.model_validate(sub_dicts[group_name])

        instance = MeasurementMetadata.model_validate(top_level)
        object.__setattr__(instance, "_raw", raw)
        return instance

    @property
    def solar_x(self) -> float | None:
        """Solar disc X coordinate in arcsec, parsed from
        ``solar_disc_coordinates``."""
        if self.solar_disc_coordinates is None:
            return None
        parts = self.solar_disc_coordinates.strip().split()
        if len(parts) < 2:  # noqa: PLR2004 - magic numbers are ok in this case
            return None
        return float(parts[0])

    @property
    def solar_y(self) -> float | None:
        """Solar disc Y coordinate in arcsec, parsed from
        ``solar_disc_coordinates``."""
        if self.solar_disc_coordinates is None:
            return None
        parts = self.solar_disc_coordinates.strip().split()
        if len(parts) < 2:  # noqa: PLR2004 - magic numbers are ok in this case
            return None
        return float(parts[1])

    def get_raw(self, key: str) -> str | None:
        """Access any raw metadata key that is not explicitly modeled."""
        return self._raw.get(key)


def _decode_info(info: np.ndarray) -> dict[str, str]:
    """Decode an Nx2 byte array into a ``{key: value}`` dict of strings."""
    result: dict[str, str] = {}
    for row in info:
        key = row[0]
        value = row[1]
        k = key.decode("UTF-8") if isinstance(key, bytes) else str(key)
        v = value.decode("UTF-8") if isinstance(value, bytes) else str(value)
        result[k] = v
    return result


def _normalize_key(suffix: str) -> str:
    """Normalize a raw info-array key suffix to a Python field name.

    Dots, spaces and hyphens are replaced with underscores and the result is
    lowercased so that raw keys like ``"TCU.retarder.wl_parameter"`` become
    ``"tcu_retarder_wl_parameter"``.
    """
    return suffix.replace(".", "_").replace(" ", "_").replace("-", "_").lower()


def _parse_zimpol_datetime(dt_str: str) -> datetime.datetime:
    """Parse a ZIMPOL datetime string.

    Format is typically ``"2024-07-13T10:22:00+01"`` (with timezone
    offset).
    """
    value = dt_str.strip()
    if not value:
        raise ValueError("Empty datetime string")

    # Accept common ZIMPOL timezone variants: +H, +HH, +HHMM, +HH:MM, and Z.
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"

    m = re.search(r"([+-])(\d{1,4})(:?\d{2})?$", value)
    if m:
        sign, hour_digits, minute_part = m.groups()
        if minute_part is not None and minute_part.startswith(":"):
            minutes = minute_part[1:]
            hours = hour_digits.zfill(2)
        elif minute_part is not None:
            hours = hour_digits.zfill(2)
            minutes = minute_part
        elif len(hour_digits) <= 2:  # noqa: PLR2004 - magic numbers are ok in this case
            hours = hour_digits.zfill(2)
            minutes = "00"
        else:
            hours = hour_digits[:2]
            minutes = hour_digits[2:].ljust(2, "0")[:2]

        value = f"{value[: m.start()]}{sign}{hours}:{minutes}"

    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid ZIMPOL datetime string: {dt_str!r}") from exc

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)

    return parsed.astimezone(datetime.timezone.utc)


class StokesParameters(BaseModel):
    """The four Stokes parameters: I, Q, U, V."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    i: np.ndarray
    q: np.ndarray
    u: np.ndarray
    v: np.ndarray

    def __iter__(self):
        """Allow unpacking: i, q, u, v = stokes."""
        return iter((self.i, self.q, self.u, self.v))


class ObservationDay(BaseModel):
    """Represents a single observation day directory."""

    model_config = ConfigDict(frozen=True)

    path: Path
    raw_dir: Path
    reduced_dir: Path
    processed_dir: Path

    @property
    def name(self) -> str:
        return self.path.name

    @property
    def date(self) -> datetime.date | None:
        """Parse the day name (YYMMDD) into a datetime.date."""
        try:
            return datetime.datetime.strptime(self.name, "%y%m%d").date()
        except ValueError:
            logger.error(
                "Invalid observation day folder name (expected YYMMDD)",
                day_name=self.name,
            )
            return None


class MaxDeltaPolicy(BaseModel):
    """Policy for determining the maximum time delta for flat-field matching.

    The default policy applies the same max_delta to all measurements.
    Subclass or replace this to implement per-wavelength or per-
    instrument policies.
    """

    default_max_delta: datetime.timedelta = Field(
        default_factory=lambda: DEFAULT_MAX_DELTA,
    )

    def get_max_delta(
        self,
        wavelength: int,
        instrument: str = "",
        telescope: str = "",
    ) -> datetime.timedelta:
        """Return the max time delta for a given measurement context.

        Override this method to implement different thresholds based on
        wavelength, instrument, telescope, etc.

        Args:
            wavelength: Measurement wavelength in Angstrom.
            instrument: Instrument name.
            telescope: Telescope name.

        Returns:
            Maximum allowed timedelta.
        """
        return self.default_max_delta


class DayProcessingResult(BaseModel):
    """Summary of processing a single observation day."""

    day_name: str
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = Field(default_factory=list)

    @property
    def total_measurements(self) -> int:
        return self.processed + self.skipped + self.failed


class CacheCleanupDayResult(BaseModel):
    """Summary of cache-file cleanup performed for one observation day.

    Attributes:
        day_name: Observation day folder name.
        checked_files: Number of ``.fits`` files found in cache directories.
        deleted_files: Number of stale files successfully deleted.
        deleted_bytes: Total size in bytes of all successfully deleted files.
        skipped_recent_files: Number of recent files kept because they are
            still within the retention window.
        skipped_bytes: Total size in bytes of all files kept within the retention
            window.
        failed_files: Number of files that could not be deleted due to an
            OS error.
    """

    model_config = ConfigDict(frozen=True)

    day_name: str
    checked_files: int = 0
    deleted_files: int = 0
    deleted_bytes: int = 0
    skipped_recent_files: int = 0
    skipped_bytes: int = 0
    failed_files: int = 0


class ScanResult(BaseModel):
    """Result of scanning a dataset root."""

    model_config = ConfigDict(frozen=True)

    observation_days: list[ObservationDay]
    pending_measurements: dict[str, list[Path]]  # day_name -> [measurement_paths]
    total_measurements: int
    total_pending: int
