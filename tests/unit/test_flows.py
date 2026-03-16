"""Tests for orchestration flow helpers."""

from pathlib import Path

from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.pipeline.scanner import ScanResult, build_scan_report_markdown


def _make_day(day_name: str) -> ObservationDay:
    day_path = Path(f"/dataset/2024/{day_name}")
    return ObservationDay(
        path=day_path,
        raw_dir=day_path / "raw",
        reduced_dir=day_path / "reduced",
        processed_dir=day_path / "processed",
    )


def test_build_scan_report_markdown_with_pending_measurements():
    scan_result = ScanResult(
        observation_days=[_make_day("240713"), _make_day("240714")],
        pending_measurements={
            "240713": [
                Path("/dataset/2024/240713/reduced/6302_m1.dat"),
                Path("/dataset/2024/240713/reduced/6302_m2.dat"),
            ]
        },
        total_measurements=5,
        total_pending=2,
    )

    report = build_scan_report_markdown(Path("/dataset"), scan_result)

    assert "Already processed: `3`" in report
    assert "Still to process: `2`" in report
    assert "| `240713` | 2 | `6302_m1.dat`, `6302_m2.dat` |" in report


def test_build_scan_report_markdown_without_pending_measurements():
    scan_result = ScanResult(
        observation_days=[_make_day("240713")],
        pending_measurements={},
        total_measurements=4,
        total_pending=0,
    )

    report = build_scan_report_markdown(Path("/dataset"), scan_result)

    assert "Already processed: `4`" in report
    assert "Still to process: `0`" in report
    assert "No pending measurements found." in report
