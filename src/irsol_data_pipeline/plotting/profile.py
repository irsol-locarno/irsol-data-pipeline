"""Profile plotting helpers for Stokes parameter measurements."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Literal

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure

from irsol_data_pipeline.core.models import MeasurementMetadata, StokesParameters
from irsol_data_pipeline.core.solar_orientation import (
    SolarOrientationInfo,
    compute_solar_orientation,
)

COLORBAR_TICK_LABEL_SIZE = 16
AXIS_LABEL_FONT_SIZE = 16


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


def _draw_solar_north_arrow(ax: Axes, info: SolarOrientationInfo) -> None:
    """Draw a solar north arrow on a profile plot panel.

    The arrow is drawn near the right edge of *ax* (at ``center_x=0.96`` in
    axes-fraction coordinates) and points in the direction of solar north
    along the (wavelength, spatial) plane.  A ``"N"`` label is placed at
    the arrowhead.

    Args:
        ax: Matplotlib :class:`~matplotlib.axes.Axes` to annotate.
        info: Pre-computed solar orientation data.
    """
    angle_rad = np.radians(info.slit_angle_solar_deg)
    dx = float(np.cos(angle_rad))
    dy = float(np.sin(angle_rad))

    # Length of the arrow in axes-fraction units.
    arrow_half = 0.10

    center_x, center_y = 0.96, 0.50

    tip_x = center_x + arrow_half * dx
    tip_y = center_y + arrow_half * dy
    tail_x = center_x - arrow_half * dx
    tail_y = center_y - arrow_half * dy

    # Draw the arrow shaft and head (no text on the arrow itself).
    ax.annotate(
        "",
        xy=(tip_x, tip_y),
        xytext=(tail_x, tail_y),
        xycoords="axes fraction",
        textcoords="axes fraction",
        arrowprops=dict(
            arrowstyle="->,head_width=0.4,head_length=0.6",
            color="yellow",
            lw=1.5,
        ),
        annotation_clip=False,
    )

    # "N" label offset slightly beyond the arrowhead.
    label_offset = 0.06
    ax.text(
        tip_x + label_offset * dx,
        tip_y + label_offset * dy,
        "N",
        transform=ax.transAxes,
        color="yellow",
        fontsize=11,
        fontweight="bold",
        ha="center",
        va="center",
        clip_on=False,
    )


def plot(
    data: StokesParameters,
    /,
    vrange_si: Sequence[float] | Literal[False] = False,
    vrange_sq: Sequence[float] | Literal[False] = False,
    vrange_su: Sequence[float] | Literal[False] = False,
    vrange_sv: Sequence[float] | Literal[False] = False,
    title: str | None = None,
    filename_save: str | Path | None = None,
    pix_low: Sequence[float] | None = None,
    pix_high: Sequence[float] | None = None,
    pix_quiet_low: Sequence[float] | None = None,
    pix_quiet_high: Sequence[float] | None = None,
    alpha_px: float = 0.21,
    colors_lines: Sequence[str] | None = None,
    a0: float | None = None,
    a1: float | None = None,
    show: bool = False,
    metadata: MeasurementMetadata | None = None,
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
        title: Optional figure title.
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
        show: Display the figure interactively after rendering.
        metadata: Optional measurement metadata. When provided, a solar north
            arrow is drawn on the Stokes I panel to indicate the direction of
            solar north in the spatial dimension.
    """

    si, sq, su, sv = data
    if colors_lines is None:
        colors_lines = ["tab:blue", "tab:orange", "tab:green", "tab:red"]

    # If no TCU has been used, Q, U and V might have an offset to consider.
    if vrange_sq is False:
        dq = 0.01
        mean_sq = np.mean(sq)
        vrange_sq = [mean_sq - dq, mean_sq + dq]
    if vrange_su is False:
        du = 0.01
        mean_su = np.mean(su)
        vrange_su = [mean_su - du, mean_su + du]
    if vrange_sv is False:
        dv = 0.01
        mean_sv = np.mean(sv)
        vrange_sv = [mean_sv - dv, mean_sv + dv]

    resolved_vrange_si = _resolve_vrange(vrange_si)
    resolved_vrange_sq = _require_vrange(vrange_sq)
    resolved_vrange_su = _require_vrange(vrange_su)
    resolved_vrange_sv = _require_vrange(vrange_sv)

    # Use pyplot-managed figure only if interactive display is needed.
    # For thread/subprocess safety, avoid pyplot state machine otherwise.
    if show:
        fig = plt.figure(figsize=(16, 14))
    else:
        fig = Figure(figsize=(16, 14))
        FigureCanvasAgg(fig)
    axes = fig.subplots(4, 1, sharex=True)
    fig.subplots_adjust(hspace=0)

    if title is not None:
        fig.suptitle(title, fontsize=24, y=0.97)

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
        )
    else:
        im0 = axes[0].imshow(
            si,
            cmap="gist_gray",
            aspect="auto",
            extent=extent,
            origin="lower",
            vmin=resolved_vrange_si[0],
            vmax=resolved_vrange_si[1],
        )
    axes[0].set_ylabel("Spatial dimension [px]")
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

    # Draw solar north arrow on Stokes I panel when metadata is available.
    if metadata is not None:
        solar_orientation = compute_solar_orientation(metadata)
        _draw_solar_north_arrow(axes[0], solar_orientation)

    # Plot Stokes Q/I.
    im1 = axes[1].imshow(
        sq,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=resolved_vrange_sq[0],
        vmax=resolved_vrange_sq[1],
        origin="lower",
    )
    axes[1].set_ylabel("Spatial dimension [px]")
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
    )
    axes[2].set_ylabel("Spatial dimension [px]")
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
    )
    axes[3].set_xlabel(str_wlt_axis)
    axes[3].set_ylabel("Spatial dimension [px]")
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
            axis="x", which="major", direction="in", length=7, width=1.5, top=True
        )
        ax.tick_params(
            axis="y", which="major", direction="in", length=7, width=1.5, right=True
        )

    fig.tight_layout(h_pad=-0.7, w_pad=0)
    if filename_save is not None:
        fig.savefig(filename_save, dpi=100, bbox_inches="tight")
    if show:
        plt.show()
        plt.close(fig)
    else:
        fig.clear()
