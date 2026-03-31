"""Unit tests for ProcessingHistory."""

from __future__ import annotations

import pytest

from irsol_data_pipeline.io.fits.processing_history import ProcessingHistory


class TestProcessingHistory:
    def test_empty_history_returns_empty_dict(self) -> None:
        history = ProcessingHistory()
        assert history.to_fits_header_entries() == {}

    def test_len_reflects_recorded_steps(self) -> None:
        history = ProcessingHistory()
        assert len(history) == 0
        history.record("step one")
        assert len(history) == 1
        history.record("step two", details="param=1")
        assert len(history) == 2

    def test_single_step_without_details(self) -> None:
        history = ProcessingHistory()
        history.record("flat-field correction")
        entries = history.to_fits_header_entries()
        assert list(entries.keys()) == ["PROC_001"]
        value, comment = entries["PROC_001"]
        assert value == "flat-field correction"
        assert "1" in comment

    def test_single_step_with_details(self) -> None:
        history = ProcessingHistory()
        history.record("wavelength calibration", details="reference_file=foo.npy")
        entries = history.to_fits_header_entries()
        value, _ = entries["PROC_001"]
        assert value == "wavelength calibration: reference_file=foo.npy"

    def test_multiple_steps_produce_sequential_keys(self) -> None:
        history = ProcessingHistory()
        history.record("flat-field correction")
        history.record("smile correction")
        history.record("wavelength calibration")
        entries = history.to_fits_header_entries()
        assert list(entries.keys()) == ["PROC_001", "PROC_002", "PROC_003"]

    def test_keys_are_zero_padded_to_three_digits(self) -> None:
        history = ProcessingHistory()
        for i in range(10):
            history.record(f"step {i}")
        entries = history.to_fits_header_entries()
        assert "PROC_001" in entries
        assert "PROC_010" in entries

    @pytest.mark.parametrize(
        "step,details,expected_value",
        [
            ("flat-field", None, "flat-field"),
            ("flat-field", "ok", "flat-field: ok"),
            ("calibration", "file=ref.npy", "calibration: file=ref.npy"),
        ],
    )
    def test_value_format(
        self,
        step: str,
        details: str | None,
        expected_value: str,
    ) -> None:
        history = ProcessingHistory()
        history.record(step, details)
        value, _ = history.to_fits_header_entries()["PROC_001"]
        assert value == expected_value

    def test_repr_contains_steps(self) -> None:
        history = ProcessingHistory()
        history.record("my step")
        assert "my step" in repr(history)
