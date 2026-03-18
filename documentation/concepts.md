# Key Concepts and Terminology

This page defines the domain vocabulary used throughout the codebase and documentation.

| Term | What it means |
|---|---|
| **Stokes parameters (I, Q, U, V)** | Four numbers that fully describe the polarisation state of light. `I` is total intensity; `Q`, `U`, `V` describe linear and circular polarisation. Each is a 2D array `(n_spatial_pixels × n_wavelength_pixels)`. |
| **Flat-field (`.dat` file starting with `ff`)** | A calibration exposure taken of a uniform light source. It encodes the spatially-varying sensitivity of the sensor (dust artefacts) and the spectral-line curvature (smile). |
| **Flat-field correction** | Dividing each measurement by the flat-field to remove dust patterns, then geometrically unwarping spectral lines to straighten them. |
| **Smile distortion** | The bending of spectral lines across the spatial axis caused by optical misalignment. Corrected using the `spectroflat` library. |
| **Wavelength calibration** | The process of assigning a physical wavelength (in Ångström) to each pixel column. The pipeline does this automatically by cross-correlating the measured spectrum against bundled reference solar spectra. |
| **`max_delta`** | The maximum allowed time difference between a measurement and the flat-field used to correct it. If no flat-field is available within this window, processing fails with an error. Default: 2 hours. |
| **Observation day** | A directory under `<root>/<year>/<day>/` that contains subdirectories `raw/`, `reduced/`, and `processed/`. |
| **`reduced/`** | Directory containing raw `.dat` measurement and flat-field files from the instrument. |
| **`processed/`** | Directory where the pipeline writes all output files. |
| **FITS** | A standard astronomical data format. |

## Info array

Every `.dat` file contains an `info` array with rich instrument and reduction metadata. See [info_array.md](info_array.md) for the full specification of its fields.
