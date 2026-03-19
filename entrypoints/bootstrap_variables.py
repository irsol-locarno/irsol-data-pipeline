"""Thin shim — delegates to irsol_data_pipeline.cli.bootstrap_variables.

Run via the package console script or directly during development:

    irsol-configure
    uv run entrypoints/bootstrap_variables.py
"""

from __future__ import annotations

from irsol_data_pipeline.cli.bootstrap_variables import app

if __name__ == "__main__":
    app()
