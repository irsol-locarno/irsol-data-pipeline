"""Tests for the metadata store (JSON read/write)."""

import json
import datetime

from irsol_data_pipeline.io.metadata_store import (
    read_metadata,
    write_error_metadata,
    write_processing_metadata,
)


class TestWriteProcessingMetadata:
    def test_writes_valid_json(self, tmp_path):
        output = tmp_path / "test_metadata.json"
        write_processing_metadata(
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

    def test_creates_parent_dirs(self, tmp_path):
        output = tmp_path / "sub" / "dir" / "meta.json"
        write_processing_metadata(
            output,
            source_file="test.dat",
            flat_field_used="ff.dat",
            flat_field_timestamp=datetime.datetime(
                2024, 6, 1, 12, 0, tzinfo=datetime.timezone.utc
            ),
            measurement_timestamp=datetime.datetime(
                2024, 6, 1, 12, 14, tzinfo=datetime.timezone.utc
            ),
            flat_field_time_delta_seconds=0,
            calibration_info={},
        )
        assert output.exists()


class TestWriteErrorMetadata:
    def test_writes_error(self, tmp_path):
        output = tmp_path / "error.json"
        write_error_metadata(
            output,
            source_file="6302_m1.dat",
            error="No flat-field within threshold",
        )

        data = json.loads(output.read_text())
        assert data["source_file"] == "6302_m1.dat"
        assert data["error"] == "No flat-field within threshold"
        assert "pipeline_version" in data


class TestReadMetadata:
    def test_reads_json(self, tmp_path):
        path = tmp_path / "test.json"
        path.write_text('{"key": "value"}')
        data = read_metadata(path)
        assert data["key"] == "value"
