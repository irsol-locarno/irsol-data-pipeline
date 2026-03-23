"""Tests for processing metadata JSON read helpers."""

from irsol_data_pipeline.io import processing_metadata as processing_metadata_io


class TestReadMetadata:
    def test_reads_json(self, tmp_path):
        path = tmp_path / "test.json"
        path.write_text('{"key": "value"}')
        data = processing_metadata_io.read(path)
        assert data["key"] == "value"
