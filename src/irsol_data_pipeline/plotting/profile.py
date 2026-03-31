"""Profile plotting helpers for Stokes parameter measurements."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.patches import Ellipse, FancyArrow, Rectangle

from irsol_data_pipeline.core.models import (
    MeasurementMetadata,
    SolarOrientationInfo,
    StokesParameters,
)

COLORBAR_TICK_LABEL_SIZE = 16
AXIS_LABEL_FONT_SIZE = 16
TITLE_FONT_SIZE = 16


def _resolve_vrange(
    vrange: Sequence[float] | Literal[False],
) -> Sequence[float] | None:
    """Resolve an optional plotting range into explicit bounds.

    Args:
        vrange: User-supplied plotting range or ``False`` for auto behavior.

    Returns:
        Explicit `[vmin, vmax]` bounds or `None` when Matplotlib should use its
        default scaling.
    """
    if vrange is False:
        return None
    return vrange


def _require_vrange(vrange: Sequence[float] | Literal[False]) -> Sequence[float]:
    """Return explicit plotting bounds after auto-range resolution.

    Args:
        vrange: User-supplied or auto-resolved plotting range.

    Returns:
        Explicit `[vmin, vmax]` bounds.

    Raises:
        ValueError: If no explicit range is available.
    """
    if vrange is False:
        raise ValueError("Expected explicit plotting range.")
    return vrange


def _draw_solar_north_arrow(fig: Figure, info: SolarOrientationInfo) -> None:
    """Draw a solar north arrow on a profile plot panel.

    The arrow is drawn near the right edge of *fig* (at ``center_x=0.96`` in
    axes-fraction coordinates) and points in the direction of solar north
    along the (wavelength, spatial) plane.  A ``"N"`` label is placed at
    the arrowhead.

    Args:
        fig: Matplotlib :class:`~matplotlib.figure.Figure` to annotate.
        info: Pre-computed solar orientation data.
    """
    x_pos = 0.55
    y_pos = 0.973
    radius = 0.04
    height = 0.002

    rectangle = Rectangle(
        (x_pos - radius / 2, y_pos - height / 2),
        width=radius,
        height=height,
        transform=fig.transFigure,
        edgecolor="red",
        facecolor="red",
        linestyle="-",
        angle=info.slit_angle_solar_deg,
        rotation_point="center",
    )
    fig.add_artist(rectangle)

    angle_rad = np.radians(info.slit_angle_solar_deg)
    # small arrow for direction
    dx = (radius / 2) * np.cos(angle_rad)
    dy = (radius / 2) * np.sin(angle_rad)
    arrow = FancyArrow(
        x_pos - dx,
        y_pos - dy,
        dx,
        dy,
        width=radius / 50,
        length_includes_head=False,
        head_width=radius / 5,
        head_length=radius / 5,
        transform=fig.transFigure,
        edgecolor="red",
        facecolor="red",
    )
    fig.add_artist(arrow)

    ellipse = Ellipse(
        (x_pos, y_pos),
        width=radius,
        height=radius,
        transform=fig.transFigure,
        edgecolor="red",
        facecolor="none",
        linestyle="-",
    )
    fig.add_artist(ellipse)

    fig.text(
        x_pos,
        y_pos + radius / 2,
        "N",
        transform=fig.transFigure,
        ha="center",
        va="center",
        fontsize=12,
    )
    fig.text(
        x_pos + radius / 2,
        y_pos,
        "W",
        transform=fig.transFigure,
        ha="center",
        va="center",
        fontsize=12,
    )
    fig.text(
        x_pos - radius / 2,
        y_pos,
        "E",
        transform=fig.transFigure,
        ha="center",
        va="center",
        fontsize=12,
    )
    fig.text(
        x_pos,
        y_pos - radius / 2,
        "S",
        transform=fig.transFigure,
        ha="center",
        va="center",
        fontsize=12,
    )


def _draw_metadata(fig: Figure, metadata: MeasurementMetadata):
    if start := metadata.datetime_start:
        date_string = start.isoformat()
        if end := metadata.datetime_end:
            date_string += f" - {end.isoformat()}"
        fig.text(
            0.01,
            0.99,
            date_string,
            transform=fig.transFigure,
            ha="left",
            va="top",
            fontsize=TITLE_FONT_SIZE,
        )

    if meas_name := metadata.name:
        fig.text(
            0.91,
            0.99,
            meas_name,
            transform=fig.transFigure,
            ha="right",
            va="top",
            fontsize=TITLE_FONT_SIZE,
        )


def plot(
    data: StokesParameters,
    /,
    vrange_si: Sequence[float] | Literal[False] = False,
    vrange_sq: Sequence[float] | Literal[False] = False,
    vrange_su: Sequence[float] | Literal[False] = False,
    vrange_sv: Sequence[float] | Literal[False] = False,
    filename_save: str | Path | None = None,
    pix_low: Sequence[float] | None = None,
    pix_high: Sequence[float] | None = None,
    pix_quiet_low: Sequence[float] | None = None,
    pix_quiet_high: Sequence[float] | None = None,
    alpha_px: float = 0.21,
    colors_lines: Sequence[str] | None = None,
    a0: float | None = None,
    a1: float | None = None,
    metadata: MeasurementMetadata | None = None,
    solar_orientation: SolarOrientationInfo | None = None,
    show: bool = False,
) -> None:
    """Plot the four Stokes components for a measurement.

    The function expects 2D Stokes arrays with shape
    ``(n_spatial_points, n_wavelengths)`` for all components.

    Args:
        data: Stokes parameters to plot.
        vrange_si: Optional ``[vmin, vmax]`` range for the Stokes I panel.
            When False, Matplotlib chooses the range automatically.
        vrange_sq: Optional ``[vmin, vmax]`` range for the Stokes Q/I panel.
            When False, a narrow range around the mean is derived automatically.
        vrange_su: Optional ``[vmin, vmax]`` range for the Stokes U/I panel.
            When False, a narrow range around the mean is derived automatically.
        vrange_sv: Optional ``[vmin, vmax]`` range for the Stokes V/I panel.
            When False, a narrow range around the mean is derived automatically.
        filename_save: Output path passed to ``Figure.savefig``.
        pix_low: Optional lower pixel bounds for highlighted spatial regions.
        pix_high: Optional upper pixel bounds for highlighted spatial regions.
        pix_quiet_low: Optional lower pixel bounds for quiet-Sun guide lines.
        pix_quiet_high: Optional upper pixel bounds for quiet-Sun guide lines.
        alpha_px: Transparency used for highlighted spatial regions.
        colors_lines: Colors used for highlighted spatial regions.
        a0: Wavelength offset in Angstrom. When both ``a0`` and ``a1`` are
            provided, the x-axis is displayed in Angstrom instead of pixels.
        a1: Wavelength dispersion in Angstrom per pixel. When both ``a0`` and
            ``a1`` are provided, the x-axis is displayed in Angstrom instead of
            pixels.
        metadata: Optional measurement metadata to annotate on the figure.
        solar_orientation: Optional pre-computed solar orientation information, used to draw a solar north arrow on the Stokes I panel when available.
        show: Display the figure interactively after rendering.
    """
    si, sq, su, sv = data
    if colors_lines is None:
        colors_lines = ["tab:blue", "tab:orange", "tab:green", "tab:red"]

    # If no TCU has been used, Q, U and V might have an offset to consider.
    if vrange_sq is False:
        center = np.median(sq)
        upper_limit = center + max(
            abs(np.percentile(sq, 99) - center),
            abs(np.percentile(sq, 1) - center),
        )
        lower_limit = center - max(
            abs(np.percentile(sq, 99) - center),
            abs(np.percentile(sq, 1) - center),
        )
        vrange_sq = [lower_limit, upper_limit]
    if vrange_su is False:
        center = np.median(su)
        upper_limit = center + max(
            abs(np.percentile(su, 99) - center),
            abs(np.percentile(su, 1) - center),
        )
        lower_limit = center - max(
            abs(np.percentile(su, 99) - center),
            abs(np.percentile(su, 1) - center),
        )
        vrange_su = [lower_limit, upper_limit]
    if vrange_sv is False:
        center = np.median(sv)
        upper_limit = center + max(
            abs(np.percentile(sv, 99) - center),
            abs(np.percentile(sv, 1) - center),
        )
        lower_limit = center - max(
            abs(np.percentile(sv, 99) - center),
            abs(np.percentile(sv, 1) - center),
        )
        vrange_sv = [lower_limit, upper_limit]

    resolved_vrange_si = _resolve_vrange(vrange_si)
    resolved_vrange_sq = _require_vrange(vrange_sq)
    resolved_vrange_su = _require_vrange(vrange_su)
    resolved_vrange_sv = _require_vrange(vrange_sv)

    # Use pyplot-managed figure only if interactive display is needed.
    # For thread/subprocess safety, avoid pyplot state machine otherwise.
    if show:
        fig = plt.figure(figsize=(14, 14))
    else:
        fig = Figure(figsize=(14, 14))
        FigureCanvasAgg(fig)
    axes = fig.subplots(4, 1, sharex=True)
    fig.subplots_adjust(hspace=0.1)

    # Define extent for imshow to set proper axes.
    if a0 is not None and a1 is not None:
        wavelength_min = a0
        wavelength_max = a0 + a1 * (si.shape[1] - 1)
        str_wlt_axis = r"Wavelength [$\AA{}$]"
    else:
        wavelength_min, wavelength_max = 0, si.shape[1]
        str_wlt_axis = "Wavelength dimension [px]"
    spatial_min, spatial_max = 0, si.shape[0]
    extent = [wavelength_min, wavelength_max, spatial_min, spatial_max]

    # Plot Stokes I.
    if resolved_vrange_si is None:
        im0 = axes[0].imshow(
            si,
            cmap="gist_gray",
            aspect="auto",
            extent=extent,
            origin="lower",
            interpolation="none",
        )
    else:
        im0 = axes[0].imshow(
            si,
            cmap="gist_gray",
            aspect="auto",
            extent=extent,
            origin="lower",
            interpolation="none",
            vmin=resolved_vrange_si[0],
            vmax=resolved_vrange_si[1],
        )
    axes[0].text(
        0.02,
        0.9,
        "I",
        transform=axes[0].transAxes,
        color="white",
        fontweight="bold",
        fontsize=15,
        bbox=dict(facecolor="black", alpha=0.5),
    )
    cbar0 = fig.colorbar(im0, ax=axes[0], orientation="vertical", pad=0.01)
    cbar0.ax.tick_params(labelsize=COLORBAR_TICK_LABEL_SIZE)

    # Draw solar north arrow on Stokes I panel when solar orientation is available.
    if solar_orientation is not None:
        _draw_solar_north_arrow(fig, solar_orientation)

    if metadata is not None:
        _draw_metadata(fig, metadata)

    # Plot Stokes Q/I.
    im1 = axes[1].imshow(
        sq,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=resolved_vrange_sq[0],
        vmax=resolved_vrange_sq[1],
        origin="lower",
        interpolation="none",
    )
    axes[1].text(
        0.02,
        0.9,
        "Q/I",
        transform=axes[1].transAxes,
        color="white",
        fontweight="bold",
        fontsize=15,
        bbox=dict(facecolor="black", alpha=0.5),
    )
    cbar1 = fig.colorbar(im1, ax=axes[1], orientation="vertical", pad=0.01)
    cbar1.ax.tick_params(labelsize=COLORBAR_TICK_LABEL_SIZE)

    # Plot Stokes U/I.
    im2 = axes[2].imshow(
        su,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=resolved_vrange_su[0],
        vmax=resolved_vrange_su[1],
        origin="lower",
        interpolation="none",
    )
    axes[2].text(
        0.02,
        0.9,
        "U/I",
        transform=axes[2].transAxes,
        color="white",
        fontweight="bold",
        fontsize=15,
        bbox=dict(facecolor="black", alpha=0.5),
    )
    cbar2 = fig.colorbar(im2, ax=axes[2], orientation="vertical", pad=0.01)
    cbar2.ax.tick_params(labelsize=COLORBAR_TICK_LABEL_SIZE)

    # Plot Stokes V/I.
    im3 = axes[3].imshow(
        sv,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=resolved_vrange_sv[0],
        vmax=resolved_vrange_sv[1],
        origin="lower",
        interpolation="none",
    )
    axes[3].set_xlabel(str_wlt_axis)
    axes[3].text(
        0.02,
        0.9,
        "V/I",
        transform=axes[3].transAxes,
        color="white",
        fontweight="bold",
        fontsize=15,
        bbox=dict(facecolor="black", alpha=0.5),
    )
    cbar3 = fig.colorbar(im3, ax=axes[3], orientation="vertical", pad=0.01)
    cbar3.ax.tick_params(labelsize=COLORBAR_TICK_LABEL_SIZE)

    # Add pixel range highlights.
    if pix_low is not None and pix_high is not None:
        for i in range(len(pix_low)):
            axes[0].axhspan(
                pix_high[i],
                pix_low[i],
                color=colors_lines[i],
                alpha=alpha_px,
                zorder=0,
            )
            axes[1].axhspan(
                pix_high[i],
                pix_low[i],
                color=colors_lines[i],
                alpha=alpha_px,
                zorder=0,
            )
            axes[2].axhspan(
                pix_high[i],
                pix_low[i],
                color=colors_lines[i],
                alpha=alpha_px,
                zorder=0,
            )
            axes[3].axhspan(
                pix_high[i],
                pix_low[i],
                color=colors_lines[i],
                alpha=alpha_px,
                zorder=0,
            )

    if pix_quiet_low is not None and pix_quiet_high is not None:
        for i in range(len(pix_quiet_low)):
            axes[0].axhline(
                pix_quiet_high[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[0].axhline(
                pix_quiet_low[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[1].axhline(
                pix_quiet_high[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[1].axhline(
                pix_quiet_low[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[2].axhline(
                pix_quiet_high[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[2].axhline(
                pix_quiet_low[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[3].axhline(
                pix_quiet_high[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )
            axes[3].axhline(
                pix_quiet_low[i],
                color="black",
                linestyle="--",
                linewidth=1,
                zorder=0,
                alpha=0.7,
            )

    for ax in axes:
        ax.xaxis.label.set_size(AXIS_LABEL_FONT_SIZE)
        ax.yaxis.label.set_size(AXIS_LABEL_FONT_SIZE)
        ax.tick_params(axis="both", labelsize=16)
        ax.tick_params(
            axis="x",
            which="major",
            direction="in",
            length=7,
            width=1.5,
            top=True,
        )
        ax.tick_params(
            axis="y",
            which="major",
            direction="in",
            length=7,
            width=1.5,
            right=True,
        )

    fig.supylabel("Spatial dimension [px]", fontsize=AXIS_LABEL_FONT_SIZE)
    # fig.tight_layout()
    if filename_save is not None:
        fig.savefig(filename_save)
    if show:
        plt.show()
        plt.close(fig)
    else:
        fig.clear()
        del fig  # Release WCS axes / map references held by the figure
