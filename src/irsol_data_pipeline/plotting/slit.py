"""Six-panel slit preview image renderer.

Generates a 2×3 figure with SDO/AIA and SDO/HMI images, each showing the
spectrograph slit position, +Q direction, mu circle, and solar limb.
"""

from __future__ import annotations

from pathlib import Path

import astropy.units as u
import matplotlib.pyplot as plt
import numpy as np
import sunpy.map
from astropy.coordinates import SkyCoord
from astropy.time import Time
from astropy.visualization import ImageNormalize, SqrtStretch
from loguru import logger
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.figure import Figure
from matplotlib.patches import Circle
from sunpy.coordinates.sun import angular_radius

from irsol_data_pipeline.core.slit_images.config import FOV_ARCSEC, SDO_DATA_LABELS
from irsol_data_pipeline.core.slit_images.coordinates import SlitGeometry


def plot(
    maps: list[tuple[str | None, sunpy.map.Map | None]],
    slit: SlitGeometry,
    output_path: Path | None,
    show: bool = False,
    show_mu_text: bool = True,
    show_mu_graphic: bool = True,
) -> None:
    """Render the 6-panel slit preview image.

    Args:
        maps: List of (time_string, SunPy Map) tuples from :func:`fetch_sdo_maps`.
        slit: Computed slit geometry.
        output_path: Optional path to save the output PNG.
        show: Whether to display the rendered figure interactively.
        show_mu_text: Whether to show mu value in the figure title.
        show_mu_graphic: Whether to draw the mu iso-contour circle.

    Raises:
        ValueError: If neither output_path nor show is requested.
    """
    if output_path is None and not show:
        raise ValueError("One of output_path or show must be requested.")

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

    xx = [slit.slit_x_start, slit.slit_x_end] * u.arcsec
    yy = [slit.slit_y_start, slit.slit_y_end] * u.arcsec

    start_utc = Time(slit.start_time, scale="utc")

    # Use pyplot-managed figure only if interactive display is needed.
    # For thread/subprocess safety, avoid pyplot state machine otherwise.
    if show:
        fig = plt.figure(figsize=(12, 8))
    else:
        fig = Figure(figsize=(12, 8))
        FigureCanvasAgg(fig)

    for i, (data_time, smap) in enumerate(maps):
        if smap is None:
            continue

        # Crop to field of view around slit center.
        try:
            bottom_left = SkyCoord(
                (slit.center_solar_x - FOV_ARCSEC // 2) * u.arcsec,
                (slit.center_solar_y - FOV_ARCSEC // 2) * u.arcsec,
                frame=smap.coordinate_frame,
            )
            top_right = SkyCoord(
                (slit.center_solar_x + FOV_ARCSEC // 2) * u.arcsec,
                (slit.center_solar_y + FOV_ARCSEC // 2) * u.arcsec,
                frame=smap.coordinate_frame,
            )
        except ValueError:
            logger.warning(
                "Cannot define crop for panel, skipping",
                panel=SDO_DATA_LABELS[i],
            )
            continue

        sub_map = smap.submap(bottom_left, top_right=top_right)

        ax = fig.add_subplot(2, 3, i + 1, projection=sub_map)

        # Plot with appropriate normalization.
        label = SDO_DATA_LABELS[i]
        slit_color = _plot_panel(ax, sub_map, label)

        # Draw +Q direction if derotator offset is available.
        if slit.derotator_offset is not None:
            _draw_q_direction(ax, slit)

        # Draw mu iso-contour circle.
        if show_mu_graphic:
            _draw_mu_circle(ax, slit, start_utc)

        # Draw solar limb.
        sub_map.draw_limb()

        # Draw slit.
        slit_display_width = 4  # visual width for visibility
        ax.plot(
            xx.to(u.deg),
            yy.to(u.deg),
            color=slit_color,
            linewidth=slit_display_width,
            alpha=0.8,
            transform=ax.get_transform("world"),
        )

        ax.set_title(label)
        if data_time:
            ax.text(
                0.99,
                0.01,
                data_time,
                transform=ax.transAxes,
                horizontalalignment="right",
                verticalalignment="bottom",
                size="x-small",
                color="white",
                bbox=dict(facecolor="black", alpha=0.5, edgecolor="none", pad=1),
            )
        ax.set_xlabel("")
        ax.set_ylabel("")

        # Lock axes to the cropped field of view.
        ax.set_xlim(
            sub_map.wcs.world_to_pixel(bottom_left)[0],
            sub_map.wcs.world_to_pixel(top_right)[0],
        )
        ax.set_ylim(
            sub_map.wcs.world_to_pixel(bottom_left)[1],
            sub_map.wcs.world_to_pixel(top_right)[1],
        )

    # Figure annotations.
    fig.suptitle(" ")
    fig.tight_layout()
    fig.text(
        0.995,
        0.99,
        f"{slit.observation_name}  {slit.measurement_name}",
        transform=fig.transFigure,
        horizontalalignment="right",
        verticalalignment="top",
        size="medium",
    )
    fig.text(
        0.005,
        0.99,
        f"{slit.start_time_str} - {slit.end_time_str}",
        transform=fig.transFigure,
        horizontalalignment="left",
        verticalalignment="top",
        size="medium",
    )
    fig.text(
        0.998,
        0.5,
        "The fine calibration of the slit position has not been done yet. "
        "The width of the slit is not accurate.",
        transform=fig.transFigure,
        horizontalalignment="right",
        verticalalignment="center",
        size="small",
        color="red",
        rotation=90,
    )
    if show_mu_text:
        fig.text(
            0.5,
            0.99,
            f"\u03bc = {slit.mu:.3f}",
            transform=fig.transFigure,
            horizontalalignment="center",
            verticalalignment="top",
            size="medium",
        )

    if output_path is not None:
        fig.savefig(str(output_path))
        logger.info("Slit preview saved", output_path=output_path)
    if show:
        plt.show()
        plt.close(fig)
    else:
        fig.clear()
        del fig  # Release WCS axes / map references held by the figure


def _plot_panel(
    ax,
    sub_map: sunpy.map.Map,
    label: str,
) -> str:
    """Plot a single SDO panel with appropriate styling.

    Returns:
        The slit color.
    """
    if label == "HMI Magnetogram":
        norm = ImageNormalize(sub_map.data, stretch=SqrtStretch(), vmin=-500, vmax=500)
        sub_map.plot(axes=ax, norm=norm)
        return "blue"
    if label == "HMI Continuum":
        sub_map.plot(axes=ax)
        return "cyan"

    sub_map.plot(axes=ax)
    return "black"


def _draw_q_direction(ax, slit: SlitGeometry) -> None:
    """Draw the +Q polarization reference direction."""
    q_angle = slit.angle_solar + slit.derotator_offset
    q_width = 1

    # Full-length dashed line.
    q_length = FOV_ARCSEC
    q_x_shift = q_length * np.cos(q_angle)
    q_y_shift = q_length * np.sin(q_angle)
    q_x = [slit.center_solar_x - q_x_shift, slit.center_solar_x + q_x_shift] * u.arcsec
    q_y = [slit.center_solar_y - q_y_shift, slit.center_solar_y + q_y_shift] * u.arcsec
    ax.plot(
        q_x.to(u.deg),
        q_y.to(u.deg),
        linestyle="dashed",
        color="yellow",
        linewidth=q_width,
        alpha=0.7,
        transform=ax.get_transform("world"),
    )

    # Arrow indicators.
    arrow_len = 0.3
    dx = arrow_len * np.cos(q_angle)
    dy = arrow_len * np.sin(q_angle)
    for sign in (1, -1):
        ax.annotate(
            "",
            xy=(0.5 + sign * dx, 0.5 + sign * dy),
            xytext=(0.5, 0.5),
            xycoords="axes fraction",
            textcoords="axes fraction",
            arrowprops=dict(
                arrowstyle="->",
                color="yellow",
                linewidth=q_width,
                alpha=0.7,
                linestyle="dashed",
            ),
        )

    # +Q label.
    ax.text(
        0.01,
        0.99,
        "+Q",
        transform=ax.transAxes,
        horizontalalignment="left",
        verticalalignment="top",
        size="x-small",
        color="yellow",
        bbox=dict(
            facecolor="black",
            alpha=0.5,
            edgecolor="yellow",
            linestyle="dashed",
            pad=1,
        ),
    )


def _draw_mu_circle(ax, slit: SlitGeometry, start_utc: Time) -> None:
    """Draw the mu iso-contour circle and label."""
    r0 = angular_radius(start_utc)
    mu_radius = r0 * np.sqrt(1 - (slit.mu**2) * np.sign(slit.mu))

    circle = Circle(
        [0, 0],
        radius=mu_radius.to_value(u.deg),
        color="white",
        linestyle="dashed",
        fill=False,
        alpha=0.5,
        transform=ax.get_transform("world"),
    )
    ax.add_patch(circle)

    ax.text(
        0.08,
        0.99,
        f"\u03bc = {slit.mu:.3f}",
        transform=ax.transAxes,
        horizontalalignment="left",
        verticalalignment="top",
        size="x-small",
        color="white",
        bbox=dict(
            facecolor="black",
            alpha=0.5,
            linestyle="dashed",
            edgecolor="white",
            pad=1,
        ),
    )
