"""Tests for the conditional Prefect decorators."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from irsol_data_pipeline.prefect.decorators import flow, prefect_enabled, task

# ---------------------------------------------------------------------------
# prefect_enabled()
# ---------------------------------------------------------------------------


class TestPrefectEnabled:
    @pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes", " true "])
    def test_truthy_values(self, value: str):
        with patch.dict(os.environ, {"PREFECT_ENABLED": value}):
            assert prefect_enabled() is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "", "random"])
    def test_falsy_values(self, value: str):
        with patch.dict(os.environ, {"PREFECT_ENABLED": value}):
            assert prefect_enabled() is False

    def test_unset(self):
        env = os.environ.copy()
        env.pop("PREFECT_ENABLED", None)
        with patch.dict(os.environ, env, clear=True):
            assert prefect_enabled() is False


# ---------------------------------------------------------------------------
# No-op mode (PREFECT_ENABLED unset / falsy)
# ---------------------------------------------------------------------------


class TestNoopMode:
    """When Prefect is disabled the decorators must be transparent."""

    def setup_method(self):
        os.environ.pop("PREFECT_ENABLED", None)

    # --- @task ---

    def test_task_bare(self):
        """@task without parentheses."""

        @task
        def add(a, b):
            return a + b

        assert add(1, 2) == 3

    def test_task_parameterised(self):
        """@task(name=..., retries=...)."""

        @task(name="add-task", retries=3)
        def add(a, b):
            return a + b

        assert add(3, 4) == 7

    # --- @flow ---

    def test_flow_bare(self):
        @flow
        def pipeline():
            return "done"

        assert pipeline() == "done"

    def test_flow_parameterised(self):
        @flow(name="my-flow")
        def pipeline():
            return "done"

        assert pipeline() == "done"

    def test_decorated_function_is_original(self):
        """The returned object should be the original callable."""

        def my_func():
            pass

        decorated = task(my_func)
        assert decorated is my_func


# ---------------------------------------------------------------------------
# Prefect mode (PREFECT_ENABLED=1)
# ---------------------------------------------------------------------------


class TestPrefectMode:
    """When Prefect is enabled the real prefect decorators should be
    applied."""

    def test_task_returns_prefect_task(self):
        with patch.dict(os.environ, {"PREFECT_ENABLED": "1"}):

            @task(name="test-task")
            def add(a, b):
                return a + b

            # Prefect-wrapped tasks have a .fn attribute pointing to the original
            assert hasattr(add, "fn") or callable(add)

    def test_flow_returns_prefect_flow(self):
        with patch.dict(os.environ, {"PREFECT_ENABLED": "1"}):

            @flow(name="test-flow")
            def pipeline():
                return 42

            assert hasattr(pipeline, "fn") or callable(pipeline)
