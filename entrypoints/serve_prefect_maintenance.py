"""Thin shim — delegates to irsol_data_pipeline.cli.serve_maintenance.

Run via the package console script or directly during development:

    irsol-serve-maintenance
    PREFECT_ENABLED=true uv run entrypoints/serve_prefect_maintenance.py
"""

from __future__ import annotations

from irsol_data_pipeline.cli.serve_maintenance import main

if __name__ == "__main__":
    main()
