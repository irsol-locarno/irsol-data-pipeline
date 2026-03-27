"""Plot-related command group integrations for the unified CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter, validators
from cyclopts.exceptions import ValidationError

from irsol_data_pipeline.cli.common import ensure_display_available
from irsol_data_pipeline.core.models import (
    CalibrationResult,
    MeasurementMetadata,
    SolarOrientationInfo,
    StokesParameters,
)
from irsol_data_pipeline.core.solar_orientation import compute_solar_orientation

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
    validator=validators.Path(ext=("png", "jpg", "jpeg"), dir_okay=False),
)
_AUTOCALIBRATE_OPTION = Parameter(
    name="autocalibrate",
    help="Whether to apply wavelength auto-calibration before plotting",
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


def _load_stokes_and_calibration_and_solar_orientation(
    input_path: Path, autocalibrate: bool
) -> tuple[
    tuple[StokesParameters, MeasurementMetadata | None],
    CalibrationResult | None,
    SolarOrientationInfo | None,
]:
    """Load Stokes data from supported input formats.

    Args:
        input_path: Input measurement path.
        autocalibrate: Wheter to perform an auto-calibration of the data if the loaded data has no information about wavelength calibration. Only applies to .fits files, .dat and .sav files are always auto-calibrated.

    Returns:
        A tuple containing Stokes parameters, an optional calibration result, and optional solar orientation information.

    Raises:
        ValidationError: If the input extension is unsupported.
    """
    suffix = input_path.suffix.lower()
    if suffix in {".dat", ".sav"}:
        from irsol_data_pipeline.io import dat as dat_io

        stokes, info = dat_io.read(input_path)
        metadata = MeasurementMetadata.from_info_array(info)
        solar_orientation = compute_solar_orientation(metadata)
        if autocalibrate:
            from irsol_data_pipeline.core.calibration.autocalibrate import (
                calibrate_measurement,
            )

            calibration = calibrate_measurement(stokes)
        else:
            calibration = None

        return (stokes, metadata), calibration, solar_orientation

    if suffix in {
        ".fits",
    }:
        from irsol_data_pipeline.io import fits as fits_io

        imported = fits_io.read(input_path)
        stokes = imported.stokes
        calibration = imported.calibration
        metadata = imported.metadata
        solar_orientation = (
            compute_solar_orientation(metadata) if metadata is not None else None
        )
        if calibration is None and autocalibrate:
            from irsol_data_pipeline.core.calibration.autocalibrate import (
                calibrate_measurement,
            )

            calibration = calibrate_measurement(stokes)

        return (stokes, metadata), calibration, solar_orientation
    raise ValidationError(
        "Unsupported input extension. Expected one of: .dat, .sav, .fits"
    )


@plot_app.command(
    name="profile",
    help="Load a .dat/.sav/.fits file and render its Stokes profile plot.",
)
def profile(
    input_file_path: Annotated[Path, _MEASUREMENT_INPUT],
    /,
    *,
    autocalibrate_option: Annotated[bool, _AUTOCALIBRATE_OPTION] = False,
    output_path_option: Annotated[Path | None, _OUTPUT_PATH_OPTION] = None,
    show: bool = False,
) -> None:
    """Render a Stokes profile plot from a raw ZIMPOL measurement file.

    Args:
        input_file_path: Existing input measurement file to load.
        autocalibrate_option: Whether to apply wavelength auto-calibration before plotting.
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
    (stokes, metadata), calibration, solar_orientation = (
        _load_stokes_and_calibration_and_solar_orientation(
            input_path, autocalibrate_option
        )
    )

    if calibration is not None:
        a0, a1 = calibration.wavelength_offset, calibration.pixel_scale
    else:
        a0, a1 = None, None

    plot_profile(
        stokes,
        filename_save=resolved_output_path,
        show=show,
        a0=a0,
        a1=a1,
        metadata=metadata,
        solar_orientation=solar_orientation,
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
