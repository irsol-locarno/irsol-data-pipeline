"""Plot-related command group integrations for the unified CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter, validators
from cyclopts.exceptions import ValidationError

plot_app = App(name="plot", help="Render plots from observation files.")

_DAT_INPUT = Parameter(
    validator=validators.Path(ext=("dat", "sav"), exists=True, dir_okay=False)
)
_PNG_OUTPUT = Parameter(validator=validators.Path(ext="png", dir_okay=False))


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


@plot_app.command(
    name="profile",
    help="Load a .dat/.sav file and render its Stokes profile plot.",
)
def profile(
    input_dat_file_path: Annotated[Path, _DAT_INPUT],
    output_path: Annotated[Path | None, _PNG_OUTPUT] = None,
    show: bool = False,
) -> None:
    """Render a Stokes profile plot from a raw ZIMPOL measurement file.

    Args:
        input_dat_file_path: Existing input .dat/.sav file to load.
        output_path: Optional output .png file to write.
        show: Display the rendered figure after saving it.
    """

    if output_path is None and not show:
        raise ValidationError("One of --show and --output-path must be set.")

    _configure_backend_for_show(show)

    from irsol_data_pipeline.io import dat as dat_io
    from irsol_data_pipeline.plotting.profile import plot as plot_profile

    input_path = input_dat_file_path.expanduser().resolve()
    resolved_output_path = (
        _resolve_output_path(output_path) if output_path is not None else None
    )
    stokes, _ = dat_io.read(input_path)
    plot_profile(stokes, filename_save=resolved_output_path, show=show)
