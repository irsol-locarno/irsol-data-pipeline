"""Thin shim — delegates to irsol_data_pipeline.cli.serve_slit_images.

Run via the package console script or directly during development:

    irsol-serve-slit-images
    PREFECT_ENABLED=true uv run entrypoints/serve_slit_image_pipeline.py
"""

from __future__ import annotations

from irsol_data_pipeline.cli.serve_slit_images import main

if __name__ == "__main__":
    main()
