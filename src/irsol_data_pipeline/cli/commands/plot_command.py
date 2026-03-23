"""Plot-related command group integrations for the unified CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter, validators
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli.common import ensure_display_available
from irsol_data_pipeline.core.models import MeasurementMetadata, StokesParameters

plot_app = App(name="plot", help="Render plots from observation files.")

_MEASUREMENT_INPUT = Parameter(
    validator=validators.Path(
        ext=(
            "dat",
            "sav",
            "fits",
        ),
        exists=True,
        dir_okay=False,
    )
)
_SLIT_INPUT = Parameter(
    validator=validators.Path(
        ext=("dat",),
        exists=True,
        dir_okay=False,
    )
)
_OUTPUT_PATH_OPTION = Parameter(
    name="output-path",
    validator=validators.Path(ext="png", dir_okay=False),
)


def _resolve_output_path(output_path: Path) -> Path:
    """Validate and normalize the requested plot output path.

    Args:
        output_path: Requested output file path.

    Returns:
        Normalized absolute output path.

    Raises:
        ValueError: If the parent directory does not exist.
    """

    resolved_output_path = output_path.expanduser().resolve()
    if not resolved_output_path.parent.exists():
        raise ValueError(
            f"Output directory does not exist: {resolved_output_path.parent}"
        )
    return resolved_output_path


def _configure_backend_for_show(show: bool) -> None:
    """Best-effort switch to an interactive backend when displaying figures.

    Args:
        show: Whether the caller requested interactive figure display.
    """

    if not show:
        return

    import matplotlib.pyplot as plt

    current_backend = plt.get_backend().lower()
    if "agg" not in current_backend:
        return

    for backend in ("TkAgg", "QtAgg", "Gtk3Agg"):
        try:
            plt.switch_backend(backend)
            return
        except Exception:
            continue


def _load_stokes_and_calibration(
    input_path: Path,
) -> tuple[StokesParameters, float | None, float | None]:
    """Load Stokes data from supported input formats.

    Args:
        input_path: Input measurement path.

    Returns:
        A tuple containing Stokes parameters and optional (a0, a1)
        wavelength calibration values.

    Raises:
        ValidationError: If the input extension is unsupported.
    """

    suffix = input_path.suffix.lower()
    if suffix in {".dat", ".sav"}:
        from irsol_data_pipeline.io import dat as dat_io

        stokes, _ = dat_io.read(input_path)
        return stokes, None, None

    if suffix in {
        ".fits",
    }:
        from irsol_data_pipeline.io import fits as fits_io

        imported = fits_io.read(input_path)
        calibration = imported.calibration
        if calibration is None:
            return imported.stokes, None, None
        return imported.stokes, calibration.wavelength_offset, calibration.pixel_scale

    raise ValidationError(
        "Unsupported input extension. Expected one of: .dat, .sav, .fits, .fit, .fts"
    )


@plot_app.command(
    name="profile",
    help="Load a .dat/.sav/.fits file and render its Stokes profile plot.",
)
def profile(
    input_file_path: Annotated[Path, _MEASUREMENT_INPUT],
    /,
    *,
    output_path_option: Annotated[Path | None, _OUTPUT_PATH_OPTION] = None,
    show: bool = False,
) -> None:
    """Render a Stokes profile plot from a raw ZIMPOL measurement file.

    Args:
        input_file_path: Existing input measurement file to load.
        output_path_option: Optional output .png file passed with `--output-path`.
        show: Display the rendered figure after saving it.
    """

    if output_path_option is None and not show:
        raise ValidationError("One of --show and --output-path must be set.")

    if show:
        ensure_display_available()

    _configure_backend_for_show(show)

    from irsol_data_pipeline.plotting.profile import plot as plot_profile

    input_path = input_file_path.expanduser().resolve()
    resolved_output_path = (
        _resolve_output_path(output_path_option)
        if output_path_option is not None
        else None
    )
    stokes, a0, a1 = _load_stokes_and_calibration(input_path)
    if a0 is not None and a1 is not None:
        plot_profile(
            stokes,
            filename_save=resolved_output_path,
            show=show,
            a0=a0,
            a1=a1,
        )
        return

    plot_profile(
        stokes,
        filename_save=resolved_output_path,
        show=show,
    )


@plot_app.command(
    name="slit",
    help="Load a .dat file and render its slit context image.",
)
def slit(
    input_file_path: Annotated[Path, _SLIT_INPUT],
    jsoc_email: str,
    output_path_option: Annotated[Path | None, _OUTPUT_PATH_OPTION] = None,
    show: bool = False,
    cache_dir: Annotated[
        Path | None, validators.Path(dir_okay=True, file_okay=False)
    ] = None,
) -> None:
    """Render a six-panel slit context image from a raw measurement file.

    Args:
        input_file_path: Existing input .dat measurement file to load.
        jsoc_email: JSOC email for DRMS queries.
        output_path_option: Optional output .png file passed with `--output-path`.
        show: Display the rendered figure after saving it.
        cache_dir: Optional cache directory for SDO data, if not provided, as temporary directoy is used.
    """

    if output_path_option is None and not show:
        raise ValidationError("One of --show and --output-path must be set.")

    if show:
        ensure_display_available()

    _configure_backend_for_show(show)

    from tempfile import gettempdir

    from irsol_data_pipeline.core.slit_images.coordinates import compute_slit_geometry
    from irsol_data_pipeline.core.slit_images.solar_data import fetch_sdo_maps
    from irsol_data_pipeline.io import dat as dat_io
    from irsol_data_pipeline.plotting.slit import plot as plot_slit

    input_path = input_file_path.expanduser().resolve()
    resolved_output_path = (
        _resolve_output_path(output_path_option)
        if output_path_option is not None
        else None
    )

    _, info = dat_io.read(input_path)
    metadata = MeasurementMetadata.from_info_array(info)

    if metadata.solar_x is None or metadata.solar_y is None:
        raise ValidationError(
            f"No solar disc coordinates in measurement {input_path.name}"
        )

    slit_geometry = compute_slit_geometry(metadata=metadata)
    maps = fetch_sdo_maps(
        start_time=slit_geometry.start_time,
        end_time=slit_geometry.end_time,
        jsoc_email=jsoc_email,
        cache_dir=cache_dir or Path(gettempdir()) / "sdo_cache",
    )

    if all(sdo_map is None for _, sdo_map in maps):
        raise ValidationError(
            f"No SDO data available for measurement {input_path.name}"
        )

    plot_slit(
        maps=maps,
        slit=slit_geometry,
        output_path=resolved_output_path,
        show=show,
    )
