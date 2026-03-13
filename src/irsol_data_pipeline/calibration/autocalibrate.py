"""Wavelength auto-calibration.

Determines the wavelength scale (pixel-to-wavelength mapping) of a
measurement by cross-correlating it against reference spectral data
and fitting spectral lines.

Adapted from the fits-generator autocalibrate module.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.signal import correlate
from scipy.optimize import curve_fit

from irsol_data_pipeline.core.calibration import CalibrationResult
from irsol_data_pipeline.core.types import StokesParameters

# V Stokes intensity threshold for row filtering
V_STOKES_CUTOFF = 0.4

# Default: reference data shipped with this package
_DEFAULT_REFDATA_DIR = Path(__file__).parent / "refdata"


def calibrate_measurement(
    stokes: StokesParameters,
    refdata_dir: Optional[Path] = None,
) -> CalibrationResult:
    """Run wavelength auto-calibration on a measurement.

    Args:
        stokes: Stokes parameters from the measurement.
        refdata_dir: Directory containing .npy reference data files.
            Uses the bundled refdata if None.

    Returns:
        CalibrationResult with fitted pixel scale and offset.

    Raises:
        RuntimeError: If calibration fails.
    """
    if refdata_dir is None:
        refdata_dir = _DEFAULT_REFDATA_DIR

    simean = _prepare_mean_spectrum(stokes.i, stokes.v)
    ref_data, reference_peaks, lines, reference_params, shift = _find_refdata(
        simean, refdata_dir
    )

    a1, a0, a1_err, a0_err, peak_pixels = _wavelength_calibration(
        reference_peaks, lines, reference_params, shift, simean
    )

    # Check calibration quality
    if a1 != 0:
        a0_err_in_pix = a0_err / abs(a1)
        a1_err_in_pix = a1_err / abs(a1)
        if a0_err_in_pix > 5 or a1_err_in_pix > 0.1:
            import warnings

            warnings.warn(
                f"High fitting error in wavelength calibration: "
                f"a0_err = {a0_err_in_pix:.2f} pixels, "
                f"a1_err = {a1_err_in_pix:.2f} pixels."
            )

    return CalibrationResult(
        pixel_scale=a1,
        wavelength_offset=a0,
        pixel_scale_error=a1_err,
        wavelength_offset_error=a0_err,
        reference_file=ref_data["filename"],
        peak_pixels=peak_pixels,
        reference_lines=lines,
    )


def _prepare_mean_spectrum(si: np.ndarray, sv: np.ndarray) -> np.ndarray:
    """Compute normalized mean spectrum, filtering high-V rows.

    Rows where the absolute Stokes V exceeds V_STOKES_CUTOFF are
    excluded from the mean to avoid contamination.

    Args:
        si: Stokes I array (2D).
        sv: Stokes V array (2D).

    Returns:
        Normalized 1D mean spectrum.
    """
    v_intensity = np.sum(np.abs(sv), axis=1)
    v_intensity = (v_intensity - np.min(v_intensity)) / (
        np.max(v_intensity) - np.min(v_intensity) + 1e-12
    )
    rows_over_threshold = set(
        np.where(np.abs(0.5 - v_intensity) >= V_STOKES_CUTOFF)[0].tolist()
    )

    simean = np.zeros(si.shape[1])
    valid_count = 0
    for i in range(si.shape[0]):
        if i in rows_over_threshold:
            continue
        simean += si[i]
        valid_count += 1

    if valid_count == 0:
        simean = np.mean(si, axis=0)
    else:
        simean = simean / valid_count

    max_val = np.max(simean)
    if max_val > 0:
        simean = simean / max_val

    return simean


def _find_refdata(
    simean: np.ndarray, refdata_dir: Path
) -> tuple[dict, np.ndarray, np.ndarray, list, float]:
    """Find the best-matching reference data by cross-correlation.

    Args:
        simean: Normalized 1D mean spectrum.
        refdata_dir: Directory containing .npy reference files.

    Returns:
        Tuple of (ref_data_dict, reference_peaks, lines, params, shift).

    Raises:
        RuntimeError: If no reference data files are found.
    """
    ref_datasets: list[dict] = []
    for f in sorted(os.listdir(refdata_dir)):
        if not f.endswith(".npy"):
            continue
        ref_data = np.load(os.path.join(refdata_dir, f), allow_pickle=True).item()
        ref_data["filename"] = f
        ref_datasets.append(ref_data)

    if not ref_datasets:
        raise RuntimeError(f"No reference data files found in {refdata_dir}")

    correlations = [None] * len(ref_datasets)
    correlation_coefficients = [0.0] * len(ref_datasets)

    for n, ref in enumerate(ref_datasets):
        ref_spectrum = ref["rs"]
        corr = correlate(
            simean - np.mean(simean),
            ref_spectrum - np.mean(ref_spectrum),
            mode="same",
        )
        correlations[n] = corr

        ref_length = ref_spectrum.size
        data_length = simean.size
        length_factor = min(1.0, data_length / ref_length)

        max_index = np.argmax(corr)
        center = corr.size / 2
        distance_from_center = abs(max_index - center)
        distance_factor = max(0.0, 1 - (distance_from_center / center))

        correlation_coefficients[n] = (
            float(np.max(corr)) * length_factor * distance_factor
        )

    best_idx = int(np.argmax(correlation_coefficients))
    ref_data = ref_datasets[best_idx]
    reference_peaks = ref_data["rp"]
    lines = ref_data["rl"]
    reference_params = ref_data["rparams"]

    shift = correlations[best_idx].size / 2 - np.argmax(correlations[best_idx])

    return ref_data, reference_peaks, np.array(lines), reference_params, shift


def _wavelength_calibration(
    reference_peaks: np.ndarray,
    lines: np.ndarray,
    reference_params: list,
    shift: float,
    simean: np.ndarray,
) -> tuple[float, float, float, float, np.ndarray]:
    """Fit wavelength calibration from reference peaks.

    Uses a linear model: wavelength = a1 * pixel + a0

    Args:
        reference_peaks: Pixel positions of peaks in reference.
        lines: Corresponding wavelength values.
        reference_params: Initial guess for [a1, a0].
        shift: Cross-correlation shift.
        simean: Normalized mean spectrum.

    Returns:
        Tuple of (a1, a0, a1_err, a0_err, peak_pixels).
    """
    pixels = reference_peaks - shift
    pixel_ranges = [np.arange(int(p) - 12, int(p) + 12, dtype=int) for p in pixels]

    for i, prange in enumerate(pixel_ranges):
        pixels[i] = _fit_line_position(prange, simean, i)

    def linear(pixel: np.ndarray, m: float, c: float) -> np.ndarray:
        return m * pixel + c

    y = curve_fit(linear, pixels, lines, reference_params)
    a1, a0 = y[0][0], y[0][1]
    a1_err, a0_err = np.sqrt(np.diag(y[1]))

    return a1, a0, a1_err, a0_err, pixels


def _fit_line_position(prange: np.ndarray, simean: np.ndarray, index: int) -> float:
    """Fit a Gaussian to a spectral line to find its sub-pixel position.

    Falls back to the discrete minimum if fitting fails.

    Args:
        prange: Pixel range around the expected line position.
        simean: Normalized mean spectrum.
        index: Line index (for diagnostics).

    Returns:
        Fitted pixel position.
    """
    if prange[-1] >= simean.shape[0] or prange[0] < 0:
        return 0.0

    minimum = np.min(simean[prange])
    A = 1 - minimum
    b = float(prange[0] + np.argmin(simean[prange]))
    c = 10.0
    args = (A, b, c)

    def gaussian(x: np.ndarray, A: float, b: float, c: float) -> np.ndarray:
        return A * np.exp(-((x - b) ** 2) / (2 * c**2)) + 1

    try:
        # Fit using 7 central points first
        central = prange[[9, 10, 11, 12, 13, 14, 15]]
        ap = curve_fit(gaussian, central, simean[central], args)
        result = ap[0][1]

        if result > b + 2 or result < b - 2:
            # Retry with full range
            ap = curve_fit(gaussian, prange, simean[prange], args)
            result = ap[0][1]

        if result > b + 2 or result < b - 2:
            result = b
    except Exception:
        result = b

    return result
