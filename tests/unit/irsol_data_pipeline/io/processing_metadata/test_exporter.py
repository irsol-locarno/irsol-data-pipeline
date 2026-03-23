"""Tests for processing metadata JSON write helpers."""

import datetime
import json

from irsol_data_pipeline.io import processing_metadata as processing_metadata_io


class TestWriteProcessingMetadata:
    def test_writes_valid_json(self, tmp_path):
        output = tmp_path / "test_metadata.json"
        processing_metadata_io.write(
            output,
            source_file="6302_m1.dat",
            flat_field_used="ff6302_m3.dat",
            flat_field_timestamp=datetime.datetime(
                2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc
            ),
            measurement_timestamp=datetime.datetime(
                2024, 6, 1, 12, 14, tzinfo=datetime.timezone.utc
            ),
            flat_field_time_delta_seconds=842.0,
            calibration_info={"pixel_scale": 0.01, "wavelength_offset": 6300.0},
        )

        assert output.exists()
        data = json.loads(output.read_text())
        assert data["source_file"] == "6302_m1.dat"
        assert data["flat_field_used"] == "ff6302_m3.dat"
        assert data["flat_field_timestamp"] == "2024-06-01T12:00:00+00:00"
        assert data["measurement_timestamp"] == "2024-06-01T12:14:00+00:00"
        assert data["flat_field_time_delta_seconds"] == 842.0
        assert "processing_timestamp" in data
        assert "pipeline_version" in data


class TestWriteErrorMetadata:
    def test_writes_error(self, tmp_path):
        output = tmp_path / "error.json"
        processing_metadata_io.write_error(
            output,
            source_file="6302_m1.dat",
            error="No flat-field within threshold",
        )

        data = json.loads(output.read_text())
        assert data["source_file"] == "6302_m1.dat"
        assert data["error"] == "No flat-field within threshold"
        assert "pipeline_version" in data
