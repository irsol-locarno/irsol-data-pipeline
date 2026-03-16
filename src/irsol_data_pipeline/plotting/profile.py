from typing import Optional

import numpy as np
from matplotlib import pyplot as plt

from irsol_data_pipeline.core.models import StokesParameters


def plot(
    data: StokesParameters,
    /,
    vrange_si=False,
    vrange_sq=False,
    vrange_su=False,
    vrange_sv=False,
    title=None,
    filename_save=None,
    pix_low=None,
    pix_high=None,
    pix_quiet_low=None,
    pix_quiet_high=None,
    alpha_px=0.21,
    colors_lines=["tab:blue", "tab:orange", "tab:green", "tab:red"],
    a0: Optional[float] = None,
    a1: Optional[float] = None,
):
    """Plot the four Stokes components for a measurement.

    The function expects 2D Stokes arrays with shape
    ``(n_spatial_points, n_wavelengths)`` for all components.

    Args:
        data: Stokes parameters to plot.
        vrange_si: Optional ``[vmin, vmax]`` range for the Stokes I panel.
            When False, matplotlib chooses the range automatically.
        vrange_sq: Optional ``[vmin, vmax]`` range for the Stokes Q/I panel.
            When False, a narrow range around the mean is derived automatically.
        vrange_su: Optional ``[vmin, vmax]`` range for the Stokes U/I panel.
            When False, a narrow range around the mean is derived automatically.
        vrange_sv: Optional ``[vmin, vmax]`` range for the Stokes V/I panel.
            When False, a narrow range around the mean is derived automatically.
        title: Optional figure title.
        filename_save: Output path passed to ``matplotlib.pyplot.savefig``.
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
    """

    si, sq, su, sv = data
    ### If no TCU has been used, then Q, U and V might have an offset that has to be considered for vrange
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

    # Create the figure with the four Stokes components
    plt.rcParams["font.size"] = 16
    fig, axes = plt.subplots(4, 1, figsize=(16, 14), sharex=True)
    plt.subplots_adjust(hspace=0)

    if title is not None:
        plt.suptitle(title, fontsize=24, y=0.97)

    # Define extent for imshow (to set proper axes)
    if a0 is not None and a1 is not None:
        wavelength_min = a0
        wavelength_max = a0 + a1 * (si.shape[1] - 1)
        str_wlt_axis = r"Wavelength [$\AA{}$]"
    else:
        wavelength_min, wavelength_max = 0, si.shape[1]
        str_wlt_axis = "Wavelength dimension [px]"
    spatial_min, spatial_max = 0, si.shape[0]
    extent = [wavelength_min, wavelength_max, spatial_min, spatial_max]

    ## Plot Stokes I
    if vrange_si is False:
        im0 = axes[0].imshow(
            si, cmap="gist_gray", aspect="auto", extent=extent, origin="lower"
        )
    else:
        im0 = axes[0].imshow(
            si,
            cmap="gist_gray",
            aspect="auto",
            extent=extent,
            origin="lower",
            vmin=vrange_si[0],
            vmax=vrange_si[1],
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
    plt.colorbar(im0, ax=axes[0], orientation="vertical", pad=0.01)

    ## Plot Stokes Q/I
    im1 = axes[1].imshow(
        sq,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=vrange_sq[0],
        vmax=vrange_sq[1],
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
    plt.colorbar(im1, ax=axes[1], orientation="vertical", pad=0.01)

    ## Plot Stokes U/I
    im2 = axes[2].imshow(
        su,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=vrange_su[0],
        vmax=vrange_su[1],
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
    plt.colorbar(im2, ax=axes[2], orientation="vertical", pad=0.01)

    ## Plot Stokes V/I
    im3 = axes[3].imshow(
        sv,
        cmap="gist_gray",
        aspect="auto",
        extent=extent,
        vmin=vrange_sv[0],
        vmax=vrange_sv[1],
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
    plt.colorbar(im3, ax=axes[3], orientation="vertical", pad=0.01)

    # Add pixel ranges highlights
    if pix_low is not None and pix_high is not None:
        for i in range(len(pix_low)):
            axes[0].axhspan(
                pix_high[i], pix_low[i], color=colors_lines[i], alpha=alpha_px, zorder=0
            )
            axes[1].axhspan(
                pix_high[i], pix_low[i], color=colors_lines[i], alpha=alpha_px, zorder=0
            )
            axes[2].axhspan(
                pix_high[i], pix_low[i], color=colors_lines[i], alpha=alpha_px, zorder=0
            )
            axes[3].axhspan(
                pix_high[i], pix_low[i], color=colors_lines[i], alpha=alpha_px, zorder=0
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

    for ax in [axes[0], axes[1], axes[2], axes[3]]:
        # ax.set_xticklabels([])
        # Also hide the x-axis ticks themselves
        # ax.tick_params(axis='x', which='both', length=0)
        ax.tick_params(
            axis="x", which="major", direction="in", length=7, width=1.5, top=True
        )
        ax.tick_params(
            axis="y", which="major", direction="in", length=7, width=1.5, right=True
        )

    # Improve overall appearance
    plt.tight_layout(h_pad=-0.7, w_pad=0)
    plt.savefig(filename_save, dpi=100, bbox_inches="tight")
