# IO Modules

The `io/` package handles all data loading and saving, providing a clean abstraction layer between the pipeline logic and the various file formats used by the IRSOL data pipeline.

> For a detailed description of the structure and content of every file the pipeline exports — including all FITS header keywords — see [Output Artefacts](output_artefacts.md).

## Supported Formats

| Format | Module | Read | Write | Description |
|--------|--------|------|-------|-------------|
| ZIMPOL `.dat` / `.sav` | `io.dat` | ✅ | — | Raw IDL save-files from the ZIMPOL instrument |
| Multi-extension FITS | `io.fits` | ✅ | ✅ | Corrected Stokes data with WCS and metadata |
| Flat-field FITS | `io.fits_flatfield` | ✅ | ✅ | Cached `FlatFieldCorrection` objects (FITS format) |
| JSON metadata | `io.processing_metadata` | ✅ | ✅ | Processing metadata and error records |


## DAT Importer


```python
def read_zimpol_dat(file_path: Path | str) -> tuple[StokesParameters, MeasurementMetadata]:
```

Reads ZIMPOL `.dat` or `.sav` files using `scipy.io.readsav()`:

- Extracts the four Stokes parameters (I, Q, U, V) from the IDL save structure.
- Returns the constructed `MeasurementMetadata` from the `info` array.
- Handles 3-D → 2-D averaging for Stokes I and V when no TCU averaging was performed.
- Raises `DatImportError` on unsupported formats or read failures.

## FITS Importer


```python
def load_fits_measurement(fits_path: Path) -> ImportedFitsMeasurement:
```

Reads corrected multi-extension FITS files produced by the pipeline:

- Loads Stokes I, Q, U, V from named HDU extensions (by `EXTNAME`) with fallback to positional indices.
- Extracts wavelength calibration from FITS header keywords (`WAVECAL`, `CRVAL3`, `CDELT3`).
- Transposes data from `(wavelength, spatial)` to `(spatial, spectral)` for consistency.
- Returns an `ImportedFitsMeasurement` frozen model with Stokes data, calibration, and header fields.
- Raises `FitsImportError` on read failures.

## FITS Exporter


```python
def write_stokes_fits(
    output_path: Path,
    stokes: StokesParameters,
    info: MeasurementMetadata,
    calibration: CalibrationResult | None,
    solar_orientation: SolarOrientationInfo | None,
    extra_header: dict[str, Any] | None = None,
) -> Path:
```

Writes corrected Stokes data as a multi-extension FITS file:

- **5-HDU structure:** Primary (metadata only) + I, Q/I, U/I, V/I image extensions.
- **SOLARNET compliance:** includes standardized header keywords.
- **WCS support:** helioprojective-Cartesian coordinates (HPLN, HPLT, AWAV axes).
- **Metadata:** telescope location, solar disc coordinates, P₀ angle, Carrington rotation.
- **Wavelength calibration:** `CDELT3`, `CRVAL3`, `CRDER3`, `CSYER3` keywords when calibration is provided.
- **Solar orientation:** `SLTANGL` in the primary header when `solar_orientation` is provided.
- **Software versions:** `SWVER` (irsol-data-pipeline), `SWVERNP` (numpy), `SWVERSP` (scipy), `SWVERSF` (spectroflat), and `SWVERPD` (pydantic) written to every HDU.
- **Custom header entries:** any key-value pairs in `extra_header` are written to the primary HDU. Use `ProcessingHistory` to record processing steps.
- **Data statistics:** min, max, median, and percentile values per extension.
- **Integrity:** `CHECKSUM` for data verification.
- Raises `FitsExportError` on write failures.

### FITS HDU Layout

| Extension | `EXTNAME` | Content |
|-----------|-----------|---------|
| 0 | `PRIMARY` | Metadata header (no data) |
| 1 | `Stokes I` | Stokes I intensity |
| 2 | `Stokes Q/I` | Stokes Q / I ratio |
| 3 | `Stokes U/I` | Stokes U / I ratio |
| 4 | `Stokes V/I` | Stokes V / I ratio |

### ProcessingHistory

```python
class ProcessingHistory:
    def record(self, step: str, details: str | None = None) -> None: ...
    def to_fits_header_entries(self) -> dict[str, tuple[Any, str]]: ...
```

A utility class for recording the sequence of processing operations applied to a `StokesParameters` object before writing to a FITS file. Each step is serialised as a numbered primary-header entry (`PROC_001`, `PROC_002`, …) when passed via `extra_header`.

```python
from irsol_data_pipeline.io.fits import ProcessingHistory, write

history = ProcessingHistory()
history.record("flat-field correction")
history.record("smile correction")
history.record("wavelength calibration", details="reference_file=ref.npy")

write(
    output_path=output_path,
    stokes=corrected_stokes,
    info=metadata,
    calibration=calibration,
    solar_orientation=solar_orientation,
    extra_header=history.to_fits_header_entries(),
)
# Primary HDU gains:
# PROC_001 = 'flat-field correction'
# PROC_002 = 'smile correction'
# PROC_003 = 'wavelength calibration: reference_file=ref.npy'
```

The `extra_header` parameter also accepts arbitrary `dict[str, scalar | (value, comment)]` mappings independent of `ProcessingHistory`.

## Flat-Field Importer / Exporter


```python
def load_correction_data(path: Path) -> FlatFieldCorrection:
def write_correction_data(output_path: Path | str, data: FlatFieldCorrection) -> Path:
```

Persists `FlatFieldCorrection` objects (containing the dust-flat array, offset map, and desmiled data) as FITS files (via `io.fits_flatfield`):

- Writes a multi-extension FITS file with `DUSTFLAT` and `DESMILED` image extensions and a primary header carrying provenance metadata.
- When an `OffsetMap` is present, writes a companion `_offset_map.fits` file via `spectroflat.smile.OffsetMap.dump()`.
- Creates parent directories automatically.
- Raises `FlatfieldCorrectionImportError` on read failures and `FlatfieldCorrectionExportError` on write failures.

## Processing Metadata


```python
def write_processing_metadata(
    output_path: Path,
    source_file: str,
    flat_field_used: str,
    flat_field_timestamp: datetime.datetime,
    measurement_timestamp: datetime.datetime,
    flat_field_time_delta_seconds: float,
    flat_field_angle: float | None,
    measuremetn_angle: float | None,
    calibration_info: dict[str, Any],
    extra: dict[str, Any] | None = None,
) -> Path:

def write_error_metadata(
    output_path: Path,
    source_file: str,
    error: str,
) -> Path:

def read_metadata(path: Path) -> dict[str, Any]:
```

### Processing Metadata JSON

Written after successful measurement processing. Contains:

- Source file name and flat-field file used.
- Timestamps and time delta between measurement and flat-field.
- Wavelength calibration parameters (a₀, a₁, errors, reference file).
- Pipeline version and processing timestamp.

### Error Metadata JSON

Written when processing fails. Contains:

- Source file name.
- Error message.
- Pipeline version and timestamp.


## Related Documentation

- [Flat-Field Correction](../core/flat_field_correction.md) — uses flat-field import/export
- [Pipeline Overview](../pipeline/pipeline_overview.md) — IO modules in the processing chain
- [Architecture](../overview/architecture.md) — IO layer in the architecture
