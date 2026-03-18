# Using the Pipeline as a Python Library

Because the scientific logic is isolated from the orchestration layer, every module can be imported and called independently. See [architecture.md](architecture.md) for a description of the layers.

## Reading a `.dat` file

```python
from pathlib import Path
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.core.models import MeasurementMetadata

stokes, info = dat_io.read(Path("6302_m1.dat"))

# stokes.i, stokes.q, stokes.u, stokes.v are 2D numpy arrays
print("Stokes I shape:", stokes.i.shape)   # e.g. (512, 1024)

# Parse the raw info array into a structured metadata object
metadata = MeasurementMetadata.from_info_array(info)
print("Wavelength:", metadata.wavelength)          # e.g. 6302
print("Observation start:", metadata.datetime_start)
```

The structure of the `info` array is documented in [info_array.md](info_array.md).

## Analysing a flat-field

```python
import numpy as np
from irsol_data_pipeline.io import dat as dat_io
from irsol_data_pipeline.core.correction.analyzer import analyze_flatfield

stokes, info = dat_io.read("ff6302_m1.dat")

# Returns (dust_flat, offset_map, desmiled)
dust_flat, offset_map, desmiled = analyze_flatfield(stokes.i)
```

## Applying flat-field correction

```python
from irsol_data_pipeline.core.correction.corrector import apply_correction

corrected_stokes = apply_correction(
    stokes=measurement_stokes,
    dust_flat=dust_flat,
    offset_map=offset_map,
)
```

## Running wavelength calibration

```python
from irsol_data_pipeline.core.calibration.autocalibrate import calibrate_measurement

calibration = calibrate_measurement(corrected_stokes)

print(f"Pixel scale:       {calibration.pixel_scale:.4f} Å/px")
print(f"Wavelength offset: {calibration.wavelength_offset:.2f} Å")
print(f"Reference file:    {calibration.reference_file}")

# Convert pixel to wavelength
pixel = 256
wavelength = calibration.pixel_to_wavelength(pixel)
print(f"Pixel {pixel} → {wavelength:.2f} Å")
```

## Reading and writing FITS files

```python
from irsol_data_pipeline.io.fits.exporter import write_stokes_fits
from irsol_data_pipeline.io.fits.importer import load_fits_measurement
from pathlib import Path

# Write
output = Path("6302_m1_corrected.fits")
write_stokes_fits(output, corrected_stokes, metadata, calibration=calibration)

# Read back
imported = load_fits_measurement(output)
print("Re-loaded Stokes I shape:", imported.stokes.i.shape)
print("Calibration:", imported.calibration)
print("Header keyword DATE-OBS:", imported.header["DATE-OBS"])
```

## Building a flat-field cache and processing a single measurement

```python
from pathlib import Path
from irsol_data_pipeline.pipeline.filesystem import discover_flatfield_files
from irsol_data_pipeline.pipeline.flatfield_cache import build_flatfield_cache
from irsol_data_pipeline.pipeline.measurement_processor import process_single_measurement
from irsol_data_pipeline.core.models import MaxDeltaPolicy
import datetime

reduced_dir = Path("data/2025/20250312/reduced")
processed_dir = Path("data/2025/20250312/processed")

ff_paths = discover_flatfield_files(reduced_dir)
policy   = MaxDeltaPolicy(default_max_delta=datetime.timedelta(hours=2))
ff_cache = build_flatfield_cache(flatfield_paths=ff_paths, max_delta=policy.default_max_delta)

process_single_measurement(
    measurement_path = reduced_dir / "6302_m1.dat",
    processed_dir    = processed_dir,
    ff_cache         = ff_cache,
    max_delta_policy = policy,
)
```

## Generating a Stokes profile plot

```python
from irsol_data_pipeline.plotting import plot_profile

plot_profile(
    corrected_stokes,
    title="6302 Å | 2025-03-12",
    filename_save="profile.png",
    a0=calibration.wavelength_offset,
    a1=calibration.pixel_scale,
)
```
