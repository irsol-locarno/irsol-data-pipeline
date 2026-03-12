"""Filesystem discovery utilities.

Functions for scanning the dataset hierarchy and discovering
observation folders, measurement files, and flat-field files.
"""

from __future__ import annotations

import re
from pathlib import Path
from irsol_data_pipeline.orchestration.decorators import task


from pydantic import BaseModel, ConfigDict


# Pattern: <wavelength>_m<id>.dat  (e.g. 6302_m1.dat)
OBSERVATION_PATTERN = re.compile(r"^(\d+)_m(\d+)\.dat$")

# Pattern: ff<wavelength>_m<id>.dat  (e.g. ff6302_m1.dat)
FLATFIELD_PATTERN = re.compile(r"^ff(\d+)_m(\d+)\.dat$")

# Patterns to ignore: cal*, dark*
IGNORED_PREFIXES = ("cal", "dark")


class ObservationDay(BaseModel):
    """Represents a single observation day directory."""

    model_config = ConfigDict(frozen=True)

    path: Path
    raw_dir: Path
    reduced_dir: Path
    processed_dir: Path

    @property
    def name(self) -> str:
        return self.path.name


@task(task_run_name="discover-observation-days-for-{root.name}")
def discover_observation_days(root: Path) -> list[ObservationDay]:
    """Scan the dataset root and discover all observation day folders.

    Expects a hierarchy of ``<root>/<year>/<day>/`` where each day
    has a ``reduced/`` subfolder.

    Args:
        root: The dataset root directory.

    Returns:
        Sorted list of ObservationDay objects.
    """
    days: list[ObservationDay] = []

    if not root.is_dir():
        return days

    for year_dir in sorted(root.iterdir()):
        if not year_dir.is_dir():
            continue
        for day_dir in sorted(year_dir.iterdir()):
            if not day_dir.is_dir():
                continue
            reduced = day_dir / "reduced"
            if reduced.is_dir():
                days.append(
                    ObservationDay(
                        path=day_dir,
                        raw_dir=day_dir / "raw",
                        reduced_dir=reduced,
                        processed_dir=day_dir / "processed",
                    )
                )

    return days


def discover_measurement_files(reduced_dir: Path) -> list[Path]:
    """Find all observation .dat files in a reduced directory.

    Excludes flat-field, calibration, and dark files.

    Args:
        reduced_dir: Path to the reduced/ folder of an observation day.

    Returns:
        Sorted list of measurement file paths.
    """
    if not reduced_dir.is_dir():
        return []

    files: list[Path] = []
    for p in sorted(reduced_dir.iterdir()):
        if not p.is_file() or not p.name.endswith(".dat"):
            continue
        # Skip flat-field files
        if FLATFIELD_PATTERN.match(p.name):
            continue
        # Skip calibration and dark files
        if any(p.name.lower().startswith(prefix) for prefix in IGNORED_PREFIXES):
            continue
        # Must match observation pattern
        if OBSERVATION_PATTERN.match(p.name):
            files.append(p)

    return files


def discover_flatfield_files(reduced_dir: Path) -> list[Path]:
    """Find all flat-field .dat files in a reduced directory.

    Args:
        reduced_dir: Path to the reduced/ folder.

    Returns:
        Sorted list of flat-field file paths.
    """
    if not reduced_dir.is_dir():
        return []

    files: list[Path] = []
    for p in sorted(reduced_dir.iterdir()):
        if p.is_file() and FLATFIELD_PATTERN.match(p.name):
            files.append(p)

    return files


def get_processed_stem(source_name: str) -> str:
    """Derive a processed file stem from a source file name.

    Example: ``6302_m1.dat`` -> ``6302_m1``

    Args:
        source_name: The source .dat filename.

    Returns:
        Base stem for processed output files.
    """
    return Path(source_name).stem


def is_measurement_processed(processed_dir: Path, source_name: str) -> bool:
    """Check whether a measurement has already been processed.

    A measurement is considered processed if either a ``*_corrected.dat.npz``
    file or an ``*_error.json`` file exists in the processed directory.

    Args:
        processed_dir: Path to the processed/ folder.
        source_name: Source .dat filename.

    Returns:
        True if already processed.
    """
    stem = get_processed_stem(source_name)
    corrected = processed_dir / f"{stem}_corrected.dat.npz"
    error = processed_dir / f"{stem}_error.json"
    return corrected.exists() or error.exists()
