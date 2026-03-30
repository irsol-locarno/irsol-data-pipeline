"""SDO solar image fetching via DRMS/JSOC.

Downloads AIA and HMI FITS images from the Joint Science Operations
Center and returns them as SunPy Map objects.
"""

from __future__ import annotations

import datetime
import tempfile
from pathlib import Path
from typing import Optional

import astropy.units as u
import drms
import requests
import sunpy.map
from astropy.io import fits
from loguru import logger

from irsol_data_pipeline.core.slit_images.config import (
    DRMS_KEYS,
    JSOC_BASE_URL,
    MAX_MISSING_PIXELS,
    SDO_DATA_PRODUCTS,
)
from irsol_data_pipeline.prefect.decorators import task


def _fetch_sdo_map_for_product_wavelength(
    keys_df,
    segs_df,
    series: str,
    segment: str,
    wavelength: int,
    mid_time: datetime.datetime,
    time_fmt: str,
    cache_dir: Optional[Path],
) -> tuple[Optional[str], Optional[sunpy.map.Map]]:
    """Fetch a single SDO map for one product/wavelength combination."""
    with logger.contextualize(series=series, wavelength=wavelength):
        best = _find_closest_record(
            keys_df, segs_df, segment, wavelength, mid_time, time_fmt
        )
        if best is None:
            logger.warning("No SDO data found")
            return (None, None)

        index, data_time, metadata = best
        slug = segs_df[segment][index]
        url = JSOC_BASE_URL + slug

        smap = _download_and_load_map(
            series, wavelength, data_time, url, metadata, cache_dir
        )
        return (data_time, smap)


def _fetch_sdo_maps_for_product(
    client,
    series: str,
    wavelengths: list[int],
    segment: str,
    time_fmt: str,
    time_range: str,
    mid_time: datetime.datetime,
    cache_dir: Optional[Path],
) -> list[tuple[Optional[str], Optional[sunpy.map.Map]]]:
    """Fetch SDO maps for all wavelengths of a single data product."""
    query = _query_drms(client, series, time_range, segment)
    if query is None:
        return [(None, None)] * len(wavelengths)

    keys_df, segs_df = query

    return [
        _fetch_sdo_map_for_product_wavelength(
            keys_df,
            segs_df,
            series,
            segment,
            wavelength,
            mid_time,
            time_fmt,
            cache_dir,
        )
        for wavelength in wavelengths
    ]


@task(task_run_name="slit-images/fetch-sdo-maps/{start_time}-{end_time}")
def fetch_sdo_maps(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    jsoc_email: str,
    cache_dir: Optional[Path] = None,
) -> list[tuple[Optional[str], Optional[sunpy.map.Map]]]:
    """Fetch SDO/AIA and SDO/HMI images for the observation time window.

    Queries the JSOC DRMS service for AIA 1600/131/193/304 and
    HMI continuum/magnetogram images closest to the observation midpoint.

    Args:
        start_time: Observation start time (UTC, timezone-naive).
        end_time: Observation end time (UTC, timezone-naive).
        jsoc_email: Email registered with JSOC for DRMS queries.
        cache_dir: Optional directory to cache downloaded FITS files.
            If provided, files are reused on subsequent calls.

    Returns:
        List of (time_string, SunPy Map) tuples, one per SDO data product.
        Map is None if data could not be fetched.

    Raises:
        RuntimeError: If the DRMS client cannot connect to JSOC.
    """

    mid_time = start_time + (end_time - start_time) / 2
    with logger.contextualize(
        start_time=start_time, end_time=end_time, mid_time=mid_time
    ):
        logger.info("Fetching SDO data")

        # Build time range with 5-minute padding
        padded_start = (start_time - datetime.timedelta(minutes=5)).strftime(
            "%Y.%m.%d_%H:%M:%S"
        )
        padded_end = (end_time + datetime.timedelta(minutes=5)).strftime(
            "%Y.%m.%d_%H:%M:%S"
        )
        time_range = f"{padded_start}-{padded_end}"

        client = drms.Client(email=jsoc_email)
        results: list[tuple[Optional[str], Optional[sunpy.map.Map]]] = []

        for series, wavelengths, segment, time_fmt in SDO_DATA_PRODUCTS:
            logger.trace(
                "Processing SDO data product",
                series=series,
                segment=segment,
                wavelengths=wavelengths,
            )
            results.extend(
                _fetch_sdo_maps_for_product(
                    client,
                    series,
                    wavelengths,
                    segment,
                    time_fmt,
                    time_range,
                    mid_time,
                    cache_dir,
                )
            )

        return results


def _query_drms(client, series: str, time_range: str, segment: str):
    """Query DRMS with retries for a given series and time range."""
    with logger.contextualize(series=series):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = client.query(
                    f"{series}[{time_range}]",
                    key=",".join(DRMS_KEYS),
                    seg=segment,
                )
                if not hasattr(result[0], "WAVELNTH"):
                    logger.warning("No data available")
                    return None
                return result
            except Exception as exc:
                logger.warning(
                    "DRMS query failed",
                    attempt=attempt + 1,
                    max_retries=max_retries,
                    error=str(exc),
                )
        return None


def _find_closest_record(
    keys_df,
    segs_df,
    segment: str,
    wavelength: int,
    mid_time: datetime.datetime,
    time_fmt: str,
) -> Optional[tuple[int, str, dict]]:
    """Find the DRMS record closest in time to the observation midpoint."""
    closest_index = None
    closest_diff = None

    for j, t_rec in enumerate(keys_df["T_REC"]):
        if segs_df[segment][j] == "NoDataDirectory":
            continue

        try:
            missvals = int(keys_df["MISSVALS"][j])
            if missvals > MAX_MISSING_PIXELS:
                continue
        except (ValueError, TypeError):
            continue

        if keys_df.WAVELNTH[j] != wavelength:
            continue

        try:
            t_rec_datetime = datetime.datetime.strptime(t_rec, time_fmt)
        except ValueError:
            continue

        diff = abs((t_rec_datetime - mid_time).total_seconds())
        if closest_diff is None or diff < closest_diff:
            closest_diff = diff
            closest_index = j

    if closest_index is None:
        return None

    data_time = keys_df["T_REC"][closest_index]
    metadata = {k: keys_df[k][closest_index] for k in DRMS_KEYS}
    return closest_index, data_time, metadata


def _download_and_load_map(
    series: str,
    wavelength: int,
    data_time: str,
    url: str,
    metadata: dict,
    cache_dir: Optional[Path],
) -> Optional[sunpy.map.Map]:
    """Download a FITS file and return a SunPy Map."""
    with logger.contextualize(
        series=series, wavelength=wavelength, data_time=data_time
    ):
        safe_time = data_time.replace("/", "-").replace(":", "-").replace(" ", "_")
        filename = f"{series}_{wavelength}_{safe_time}.fits"

        target: Optional[Path] = None
        if cache_dir is not None:
            cache_dir.mkdir(parents=True, exist_ok=True)
            target = cache_dir / filename
            logger.debug("Checking for cached SDO file", target=target)

        if target is not None and target.is_file():
            logger.debug("Using cached SDO file", target=target)
        else:
            logger.info(
                "Cache unavailable, or cached file not found. Downloading SDO data",
                target=target,
                url=url,
            )
            try:
                resp = requests.get(url, timeout=120)
                resp.raise_for_status()
            except requests.RequestException:
                logger.exception("Failed to download SDO data", url=url)
                return None

            if target is not None:
                logger.debug("Writing response to target file", target=target)
                target.write_bytes(resp.content)
            else:
                # Use a temp approach; write to cache_dir if available
                with tempfile.NamedTemporaryFile(
                    suffix=".fits", delete=False, dir=cache_dir
                ) as tmp_file:
                    logger.debug(
                        "Writing response to temporary file", temp_path=tmp_file.name
                    )
                    tmp_file.write(resp.content)
                    target = Path(tmp_file.name)

        try:
            data, header = fits.getdata(str(target), header=True)
        except Exception:
            logger.exception("Error reading FITS file", target=target)
            return None

        for k, v in metadata.items():
            header[k] = v

        is_hmi = "hmi" in series
        if is_hmi:
            header["CDELT1"] = -header["CDELT1"]
            header["CDELT2"] = -header["CDELT2"]

        smap = sunpy.map.Map(data, header)

        if is_hmi:
            smap = smap.rotate(angle=180 * u.Unit("deg"))

        return smap
