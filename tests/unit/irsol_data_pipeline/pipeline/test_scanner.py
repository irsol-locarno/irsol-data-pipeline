"""Tests for the scanner module."""

from __future__ import annotations

from pathlib import Path

from irsol_data_pipeline.core.models import ObservationDay
from irsol_data_pipeline.pipeline.scanner import (
    ScanResult,
    build_scan_flatfield_report_markdown,
    build_slit_scan_report_markdown,
    scan_flatfield_dataset,
    scan_slit_dataset,
)


def _make_day(day_name: str) -> ObservationDay:
    day_path = Path(f"/dataset/2024/{day_name}")
    return ObservationDay(
        path=day_path,
        raw_dir=day_path / "raw",
        reduced_dir=day_path / "reduced",
        processed_dir=day_path / "processed",
    )


class TestScanDataset:
    def test_empty_root(self, tmp_path):
        result = scan_flatfield_dataset(tmp_path)
        assert result.total_measurements == 0
        assert result.total_pending == 0
        assert len(result.observation_days) == 0

    def test_discovers_pending(self, tmp_path):
        # Create: root/2024/240713/reduced/6302_m1.dat
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        reduced.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (reduced / "6302_m2.dat").touch()
        (reduced / "ff6302_m1.dat").touch()

        result = scan_flatfield_dataset(tmp_path)
        assert result.total_measurements == 2
        assert result.total_pending == 2
        assert "240713" in result.pending_measurements

    def test_skips_processed(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        processed = day / "processed"
        reduced.mkdir(parents=True)
        processed.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (reduced / "6302_m2.dat").touch()
        # Mark m1 as processed
        (processed / "6302_m1_corrected.fits").touch()

        result = scan_flatfield_dataset(tmp_path)
        assert result.total_measurements == 2
        assert result.total_pending == 1

    def test_skips_errored(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        processed = day / "processed"
        reduced.mkdir(parents=True)
        processed.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (processed / "6302_m1_error.json").touch()

        result = scan_flatfield_dataset(tmp_path)
        assert result.total_pending == 0


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

    report = build_scan_flatfield_report_markdown(Path("/dataset"), scan_result)

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

    report = build_scan_flatfield_report_markdown(Path("/dataset"), scan_result)

    assert "Already processed: `4`" in report
    assert "Still to process: `0`" in report
    assert "No pending measurements found." in report


class TestScanSlitDataset:
    def test_empty_root(self, tmp_path):
        result = scan_slit_dataset(tmp_path)
        assert result.total_measurements == 0
        assert result.total_pending == 0
        assert len(result.observation_days) == 0

    def test_discovers_pending(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        reduced.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (reduced / "6302_m2.dat").touch()
        (reduced / "ff6302_m1.dat").touch()  # flat-field file, ignored

        result = scan_slit_dataset(tmp_path)
        assert result.total_measurements == 2
        assert result.total_pending == 2
        assert "240713" in result.pending_measurements

    def test_skips_already_generated(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        processed = day / "processed"
        reduced.mkdir(parents=True)
        processed.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (reduced / "6302_m2.dat").touch()
        # Mark m1 as already having a slit preview
        (processed / "6302_m1_slit_preview.png").touch()

        result = scan_slit_dataset(tmp_path)
        assert result.total_measurements == 2
        assert result.total_pending == 1
        assert result.pending_measurements["240713"] == [reduced / "6302_m2.dat"]

    def test_skips_errored(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        processed = day / "processed"
        reduced.mkdir(parents=True)
        processed.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (processed / "6302_m1_slit_preview_error.json").touch()

        result = scan_slit_dataset(tmp_path)
        assert result.total_pending == 0

    def test_day_with_no_pending_is_excluded_from_pending_measurements(self, tmp_path):
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        processed = day / "processed"
        reduced.mkdir(parents=True)
        processed.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()
        (processed / "6302_m1_slit_preview.png").touch()

        result = scan_slit_dataset(tmp_path)
        assert result.total_pending == 0
        assert "240713" not in result.pending_measurements

    def test_predicate_filters_days(self, tmp_path):
        """Days rejected by the predicate are not included in the scan."""
        day = tmp_path / "2024" / "240713"
        reduced = day / "reduced"
        reduced.mkdir(parents=True)
        (reduced / "6302_m1.dat").touch()

        result = scan_slit_dataset(tmp_path, predicate=lambda _: False)
        assert len(result.observation_days) == 0
        assert result.total_measurements == 0
        assert result.total_pending == 0

    def test_multiple_days_only_pending_submitted(self, tmp_path):
        """Days with all measurements already generated are not pending."""
        for day_name, has_preview in [("240713", True), ("240714", False)]:
            day = tmp_path / "2024" / day_name
            reduced = day / "reduced"
            processed = day / "processed"
            reduced.mkdir(parents=True)
            processed.mkdir(parents=True)
            (reduced / "6302_m1.dat").touch()
            if has_preview:
                (processed / "6302_m1_slit_preview.png").touch()

        result = scan_slit_dataset(tmp_path)
        assert result.total_measurements == 2
        assert result.total_pending == 1
        assert "240713" not in result.pending_measurements
        assert "240714" in result.pending_measurements


def test_build_slit_scan_report_markdown_with_pending_measurements():
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

    report = build_slit_scan_report_markdown(Path("/dataset"), scan_result)

    assert "Slit Image Generation Scan Summary" in report
    assert "Already generated: `3`" in report
    assert "Still to generate: `2`" in report
    assert "| `240713` | 2 | `6302_m1.dat`, `6302_m2.dat` |" in report


def test_build_slit_scan_report_markdown_without_pending_measurements():
    scan_result = ScanResult(
        observation_days=[_make_day("240713")],
        pending_measurements={},
        total_measurements=4,
        total_pending=0,
    )

    report = build_slit_scan_report_markdown(Path("/dataset"), scan_result)

    assert "Slit Image Generation Scan Summary" in report
    assert "Already generated: `4`" in report
    assert "Still to generate: `0`" in report
    assert "No pending slit preview measurements found." in report
