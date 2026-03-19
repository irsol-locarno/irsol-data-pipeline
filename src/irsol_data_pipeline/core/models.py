"""Domain model."""

from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import Optional

import numpy as np
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
    peak_pixels: Optional[np.ndarray] = None  # pixel positions of fitted peaks
    reference_lines: Optional[np.ndarray] = None  # wavelengths of the reference lines

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


def _parse_yes_no(value: object) -> Optional[bool]:
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

    software: Optional[str] = None
    status: Optional[bool] = None
    file: Optional[str] = None
    number_of_files: Optional[int] = None
    file_dc_used: Optional[str] = None
    dcfit: Optional[str] = None
    demodulation_matrix: Optional[str] = None
    order_of_rows: list[int] = Field(default_factory=list)
    mode: Optional[str] = None
    tcu_method: Optional[str] = None
    pixels_replaced: Optional[str] = None
    outfname: Optional[str] = None

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: object) -> Optional[bool]:
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

    software: Optional[str] = None
    file: Optional[str] = None
    status: Optional[bool] = None
    description: Optional[str] = None

    @field_validator("status", mode="before")
    @classmethod
    def _coerce_status(cls, v: object) -> Optional[bool]:
        return _parse_yes_no(v)


class CameraInfo(BaseModel):
    """Metadata from ``measurement.camera.*`` keys."""

    model_config = ConfigDict(frozen=True)

    identity: Optional[str] = None
    ccd: Optional[str] = None
    temperature: Optional[float] = None
    position: Optional[str] = None


class SpectrographInfo(BaseModel):
    """Metadata from ``measurement.spectrograph.*`` keys."""

    model_config = ConfigDict(frozen=True)

    alpha: Optional[float] = None
    grtwl: Optional[float] = None
    order: Optional[int] = None
    slit: Optional[float] = None


class DerotatorInfo(BaseModel):
    """Metadata from ``measurement.derotator.*`` keys."""

    model_config = ConfigDict(frozen=True)

    coordinate_system: Optional[int] = None
    position_angle: Optional[float] = None
    offset: Optional[float] = None


class TCUInfo(BaseModel):
    """Metadata from ``measurement.TCU.*`` keys."""

    model_config = ConfigDict(frozen=True)

    mode: Optional[int] = None
    retarder_name: Optional[str] = None
    retarder_wl_parameter: Optional[str] = None
    positions: Optional[str] = None


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
        frozen=True, arbitrary_types_allowed=True, populate_by_name=True
    )

    # --- measurement core ---
    file: Optional[str] = None
    telescope_name: str
    instrument_post_focus: Optional[str] = None
    instrument: str
    modulator_type: Optional[str] = None
    project: str = ""
    observer: str = ""
    wavelength: int
    name: str
    # Datetimes are parsed and already in UTC timezone. The original raw string is kept in _raw for reference.
    datetime_start: datetime.datetime = Field(validation_alias="datetime")
    datetime_end: Optional[datetime.datetime] = None
    type: str
    id: int
    sequence_length: Optional[int] = None
    sub_sequence_length: Optional[int] = None
    sub_sequence_name: Optional[str] = None
    stokes_vector: Optional[str] = None
    integration_time: Optional[float] = None
    images: list[int] = Field(default_factory=list)
    image_type: Optional[str] = None
    image_type_x: Optional[str] = None
    image_type_y: Optional[str] = None
    guiding_status: Optional[int] = None
    pig_intensity: Optional[int] = None
    solar_disc_coordinates: Optional[str] = None
    solar_p0: Optional[float] = Field(default=None, validation_alias="sun_p0")
    limbguider_status: Optional[int] = None
    polcomp_status: Optional[int] = None

    # --- sub-models ---
    camera: CameraInfo = Field(default_factory=CameraInfo)
    spectrograph: SpectrographInfo = Field(default_factory=SpectrographInfo)
    derotator: DerotatorInfo = Field(default_factory=DerotatorInfo)
    tcu: TCUInfo = Field(default_factory=TCUInfo)
    reduction: ReductionInfo = Field(default_factory=ReductionInfo)
    calibration: CalibrationInfo = Field(default_factory=CalibrationInfo)

    # --- top-level flags outside the groups above ---
    flatfield_status: Optional[bool] = None
    global_noise: Optional[str] = None
    global_mean: Optional[str] = None

    # Keep the raw decoded dict for any field we haven't explicitly modeled.
    _raw: dict[str, str] = PrivateAttr(default_factory=dict)

    @field_validator("flatfield_status", mode="before")
    @classmethod
    def _coerce_flatfield_status(cls, v: object) -> Optional[bool]:
        return _parse_yes_no(v)

    @field_validator("datetime_start", "datetime_end", mode="before")
    @classmethod
    def _coerce_datetime(cls, v: object) -> Optional[datetime.datetime]:
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
    def from_info_array(info: np.ndarray) -> "MeasurementMetadata":
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
    def solar_x(self) -> Optional[float]:
        """Solar disc X coordinate in arcsec, parsed from
        ``solar_disc_coordinates``."""
        if self.solar_disc_coordinates is None:
            return None
        parts = self.solar_disc_coordinates.strip().split()
        if len(parts) < 2:
            return None
        return float(parts[0])

    @property
    def solar_y(self) -> Optional[float]:
        """Solar disc Y coordinate in arcsec, parsed from
        ``solar_disc_coordinates``."""
        if self.solar_disc_coordinates is None:
            return None
        parts = self.solar_disc_coordinates.strip().split()
        if len(parts) < 2:
            return None
        return float(parts[1])

    def get_raw(self, key: str) -> Optional[str]:
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
        elif len(hour_digits) <= 2:
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


class MaxDeltaPolicy(BaseModel):
    """Policy for determining the maximum time delta for flat-field matching.

    The default policy applies the same max_delta to all measurements.
    Subclass or replace this to implement per-wavelength or per-
    instrument policies.
    """

    default_max_delta: datetime.timedelta = Field(
        default_factory=lambda: DEFAULT_MAX_DELTA
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
        checked_files: Number of ``.pkl`` files found in cache directories.
        deleted_files: Number of stale files successfully deleted.
        skipped_recent_files: Number of recent files kept because they are
            still within the retention window.
        failed_files: Number of files that could not be deleted due to an
            OS error.
    """

    model_config = ConfigDict(frozen=True)

    day_name: str
    checked_files: int = 0
    deleted_files: int = 0
    skipped_recent_files: int = 0
    failed_files: int = 0


class ScanResult(BaseModel):
    """Result of scanning a dataset root."""

    model_config = ConfigDict(frozen=True)

    observation_days: list[ObservationDay]
    pending_measurements: dict[str, list[Path]]  # day_name -> [measurement_paths]
    total_measurements: int
    total_pending: int
