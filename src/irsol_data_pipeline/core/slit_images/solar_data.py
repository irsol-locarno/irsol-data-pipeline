"""SDO solar image fetching via DRMS/JSOC.

Downloads AIA and HMI FITS images from the Joint Science Operations
Center and returns them as SunPy Map objects.
"""

from __future__ import annotations

import datetime
import tempfile
from pathlib import Path

import astropy.units as u
import drms
import requests
import sunpy.map
from astropy.io import fits
from loguru import logger
from tenacity import RetryCallState, retry, stop_after_attempt, wait_exponential

from irsol_data_pipeline.core.slit_images.config import (
    DRMS_KEYS,
    JSOC_BASE_URL,
    MAX_MISSING_PIXELS,
    SDO_DATA_PRODUCTS,
)


def _fetch_sdo_map_for_product_wavelength(
    keys_df,
    segs_df,
    series: str,
    segment: str,
    wavelength: int,
    mid_time: datetime.datetime,
    time_fmt: str,
    cache_dir: Path | None,
) -> tuple[str | None, sunpy.map.Map | None]:
    """Fetch a single SDO map for one product/wavelength combination."""
    with logger.contextualize(series=series, wavelength=wavelength):
        best = _find_closest_record(
            keys_df,
            segs_df,
            segment,
            wavelength,
            mid_time,
            time_fmt,
        )
        if best is None:
            logger.warning("No SDO data found")
            return (None, None)

        index, data_time, metadata = best
        slug = segs_df[segment][index]
        url = JSOC_BASE_URL + slug

        smap = _download_and_load_map(
            series,
            wavelength,
            data_time,
            url,
            metadata,
            cache_dir,
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
    cache_dir: Path | None,
) -> list[tuple[str | None, sunpy.map.Map | None]]:
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


def fetch_sdo_maps(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    jsoc_email: str,
    cache_dir: Path | None = None,
) -> list[tuple[str | None, sunpy.map.Map | None]]:
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
        start_time=start_time,
        end_time=end_time,
        mid_time=mid_time,
    ):
        logger.info("Fetching SDO data")

        # Build time range with 5-minute padding
        padded_start = (start_time - datetime.timedelta(minutes=5)).strftime(
            "%Y.%m.%d_%H:%M:%S",
        )
        padded_end = (end_time + datetime.timedelta(minutes=5)).strftime(
            "%Y.%m.%d_%H:%M:%S",
        )
        time_range = f"{padded_start}-{padded_end}"

        client = drms.Client(email=jsoc_email)
        results: list[tuple[str | None, sunpy.map.Map | None]] = []

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
                ),
            )

        return results


def _log_retry(retry_state: RetryCallState) -> None:
    """Log a tenacity retry attempt using loguru."""
    logger.warning(
        "Retrying after failure",
        attempt=retry_state.attempt_number,
        error=str(retry_state.outcome.exception()),
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=_log_retry,
    reraise=True,
)
def _execute_drms_query(
    client: drms.Client,
    series: str,
    time_range: str,
    segment: str,
) -> tuple:
    """Execute a single DRMS query attempt against the JSOC server.

    Args:
        client: DRMS client instance.
        series: DRMS series name.
        time_range: DRMS time range string.
        segment: DRMS segment name.

    Returns:
        Tuple of (keys DataFrame, segments DataFrame) returned by the client.
    """
    return client.query(
        f"{series}[{time_range}]",
        key=",".join(DRMS_KEYS),
        seg=segment,
    )


def _query_drms(
    client: drms.Client,
    series: str,
    time_range: str,
    segment: str,
) -> tuple | None:
    """Query DRMS with tenacity retries for a given series and time range."""
    with logger.contextualize(series=series):
        try:
            result = _execute_drms_query(client, series, time_range, segment)
        except Exception:
            logger.warning("DRMS query failed after all retries")
            return None
        if not hasattr(result[0], "WAVELNTH"):
            logger.warning("No data available")
            return None
        return result


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=_log_retry,
    reraise=True,
)
def _fetch_fits_file(url: str, target: Path) -> int:
    """Download a FITS file from the given URL and save it to target path.

    Retries up to 3 times with exponential back-off on any network error.

    Args:
        url: URL to download the FITS file from.
        target: Local path to write the downloaded bytes to.

    Returns:
        Total number of bytes written.

    Raises:
        requests.HTTPError: If the server returns an HTTP error status.
        requests.RequestException: If all retry attempts are exhausted.
    """
    bytes_written = 0
    bytes_2mb = 2 * 1024 * 1024
    with requests.get(url, timeout=120, stream=True) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        logger.trace("Starting streamed download", total_bytes=total, url=url)
        with open(target, "wb") as f:
            for chunk in resp.iter_content(chunk_size=bytes_2mb):
                if chunk:
                    f.write(chunk)
                    bytes_written += len(chunk)
                    if total:
                        logger.trace(
                            "Download progress",
                            bytes_written=bytes_written,
                            percent=round(100 * bytes_written / total, 2),
                        )
    return bytes_written


def _find_closest_record(
    keys_df,
    segs_df,
    segment: str,
    wavelength: int,
    mid_time: datetime.datetime,
    time_fmt: str,
) -> tuple[int, str, dict] | None:
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
    cache_dir: Path | None,
) -> sunpy.map.Map | None:
    """Download a FITS file and return a SunPy Map."""
    with logger.contextualize(
        series=series,
        wavelength=wavelength,
        data_time=data_time,
    ):
        safe_time = data_time.replace("/", "-").replace(":", "-").replace(" ", "_")
        filename = f"{series}_{wavelength}_{safe_time}.fits"

        target: Path | None = None
        is_temp_file = False
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
            if target is None:
                # No cache directory — use a temporary file that we clean up
                # ourselves once the FITS data has been loaded into memory.
                with tempfile.NamedTemporaryFile(
                    suffix=".fits",
                    delete=False,
                ) as tmp_file:
                    target = Path(tmp_file.name)
                is_temp_file = True

            try:
                bytes_written = _fetch_fits_file(url, target)
                logger.trace(
                    "Download complete",
                    total_bytes=bytes_written,
                    target=str(target),
                )
            except Exception:
                logger.exception("Failed to download the FITS file", url=url)
                if is_temp_file:
                    target.unlink(missing_ok=True)
                return None

        try:
            data, header = fits.getdata(str(target), header=True)
        except Exception:
            logger.exception("Error reading FITS file", target=target)
            return None
        finally:
            # Remove the temporary FITS file now that its contents are in
            # memory — whether the read succeeded or failed.
            if is_temp_file:
                target.unlink(missing_ok=True)

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
