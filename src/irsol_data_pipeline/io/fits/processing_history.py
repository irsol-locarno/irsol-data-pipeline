"""Utility class for recording and exporting processing steps to FITS headers.

:class:`ProcessingHistory` allows callers to record the sequence of operations
applied to a :class:`~irsol_data_pipeline.core.models.StokesParameters` object
before writing to a FITS file.  Calling :meth:`ProcessingHistory.to_fits_header_entries`
returns a dictionary of key-value pairs ready to pass as the *extra_header*
argument of :func:`~irsol_data_pipeline.io.fits.exporter.write_stokes_fits`.
"""

from __future__ import annotations


class ProcessingHistory:
    """Records processing steps and serialises them as FITS header entries.

    Each recorded step is assigned a sequential key of the form ``PROC_NNN``
    (where *NNN* is a zero-padded three-digit counter.
    The corresponding value is a human-readable description built
    from the step name and optional details string.

    Example::

        history = ProcessingHistory()
        history.record("flat-field correction")
        history.record("smile correction")
        history.record("wavelength calibration", details="reference_file=foo.npy")

        write_stokes_fits(
            output_path=output_path,
            stokes=corrected_stokes,
            info=metadata,
            calibration=calibration,
            solar_orientation=solar_orientation,
            extra_header=history.to_fits_header_entries(),
        )
    """

    def __init__(self) -> None:
        """Initialise an empty processing history."""
        self._steps: list[tuple[str, str | None]] = []

    def record(self, step: str, details: str | None = None) -> None:
        """Record a processing step.

        Args:
            step: Short human-readable name of the processing step.
            details: Optional extra information about the step (e.g. parameter
                values or filenames).
        """
        self._steps.append((step, details))

    def to_fits_header_entries(self) -> dict[str, tuple[str, str]]:
        """Return the recorded steps as FITS primary-header key-value pairs.

        Each entry is a ``(value, comment)`` tuple compatible with the
        *extra_header* argument of
        :func:`~irsol_data_pipeline.io.fits.exporter.write_stokes_fits`.

        Keys are zero-padded strings of the form ``PROC_NNN``, where *NNN* is
        the one-based index of the step.

        Returns:
            Ordered mapping from FITS keyword to ``(value, comment)`` tuple.
        """
        entries: dict[str, tuple[str, str]] = {}
        for i, (step, details) in enumerate(self._steps, start=1):
            key = f"PROC_{i:03d}"
            value = f"{step}: {details}" if details is not None else step
            entries[key] = (value, f"Processing step {i}")
        return entries

    def __len__(self) -> int:
        """Return the number of recorded steps."""
        return len(self._steps)

    def __repr__(self) -> str:
        """Return a developer-friendly representation."""
        return f"ProcessingHistory({self._steps!r})"
