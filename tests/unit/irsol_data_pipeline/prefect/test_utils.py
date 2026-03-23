"""Tests for prefect utility helpers."""

from __future__ import annotations

from irsol_data_pipeline.prefect.utils import _flatten_dict


class TestFlattenDict:
    def test_flat_dict(self):
        result = _flatten_dict({"a": 1, "b": "hello"})
        assert result == [{"key": "a", "value": "1"}, {"key": "b", "value": "hello"}]

    def test_one_level_nested(self):
        result = _flatten_dict({"a": {"b": 1, "c": 2}})
        assert result == [
            {"key": "a.b", "value": "1"},
            {"key": "a.c", "value": "2"},
        ]

    def test_deeply_nested(self):
        result = _flatten_dict({"a": {"b": {"c": {"d": 42}}}})
        assert result == [{"key": "a.b.c.d", "value": "42"}]

    def test_mixed_flat_and_nested(self):
        result = _flatten_dict({"x": 1, "y": {"z": 2}})
        assert result == [
            {"key": "x", "value": "1"},
            {"key": "y.z", "value": "2"},
        ]

    def test_multiple_nested_siblings(self):
        result = _flatten_dict({"a": {"b": 1}, "c": {"d": 2}})
        assert result == [
            {"key": "a.b", "value": "1"},
            {"key": "c.d", "value": "2"},
        ]

    def test_empty_dict(self):
        assert _flatten_dict({}) == []

    def test_empty_nested_dict(self):
        assert _flatten_dict({"a": {}}) == []

    def test_non_string_values_are_stringified(self):
        result = _flatten_dict({"a": 3.14, "b": True, "c": None})
        assert result == [
            {"key": "a", "value": "3.14"},
            {"key": "b", "value": "True"},
            {"key": "c", "value": "None"},
        ]

    def test_prefix_is_prepended(self):
        result = _flatten_dict({"b": 1}, prefix="a")
        assert result == [{"key": "a.b", "value": "1"}]

    def test_prefix_propagates_through_nesting(self):
        result = _flatten_dict({"b": {"c": 1}}, prefix="a")
        assert result == [{"key": "a.b.c", "value": "1"}]
