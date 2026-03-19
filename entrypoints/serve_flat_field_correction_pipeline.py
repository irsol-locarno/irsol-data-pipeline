"""Thin shim — delegates to
irsol_data_pipeline.cli.serve_flat_field_correction.

Run via the package console script or directly during development:

    irsol-serve-flat-field-correction
    PREFECT_ENABLED=true uv run entrypoints/serve_flat_field_correction_pipeline.py
"""

from __future__ import annotations

from irsol_data_pipeline.cli.serve_flat_field_correction import main

if __name__ == "__main__":
    main()
