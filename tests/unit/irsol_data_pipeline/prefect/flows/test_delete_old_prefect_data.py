"""Unit tests for the maintenance flow that deletes old Prefect flow runs."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Coroutine
from typing import Any, TypeVar

import httpx
import pytest
from prefect.exceptions import PrefectHTTPStatusError

from irsol_data_pipeline.prefect.flows.maintenance import delete_old_prefect_data as mod


def _make_503_error() -> PrefectHTTPStatusError:
    """Build the 503 the Prefect server returns on SQLite lock contention."""
    request = httpx.Request("DELETE", "http://127.0.0.1:4200/api/flow_runs/x")
    response = httpx.Response(
        503,
        json={"exception_message": "Service Unavailable"},
        request=request,
    )
    return PrefectHTTPStatusError(
        "Server error '503 Service Unavailable' for url 'http://127.0.0.1:4200/api/flow_runs/x'",
        request=request,
        response=response,
    )


class _FakeDeleteClient:
    """Async context manager faking ``get_client`` for the delete task.

    Raises ``failures`` times with a 503, then succeeds.
    """

    def __init__(self, failures: int, error: Exception | None = None) -> None:
        self.failures = failures
        self.error = error
        self.calls = 0

    async def __aenter__(self) -> _FakeDeleteClient:
        return self

    async def __aexit__(self, *exc_info: Any) -> bool:
        return False

    async def delete_flow_run(self, flow_run_id: uuid.UUID) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error if self.error is not None else _make_503_error()


@pytest.fixture(autouse=True)
def _no_backoff_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero out the retry backoff so tests run instantly."""
    monkeypatch.setattr(mod, "_DELETE_BACKOFF_SECONDS", 0.0)


T = TypeVar("T")


def _run(task_result: Coroutine[Any, Any, T]) -> T:
    """Run the coroutine returned by an async task's ``.fn`` attribute."""
    return asyncio.run(task_result)


class TestDeleteFlowRunIdRetry:
    def test_succeeds_after_transient_503(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flow_run_id = uuid.uuid4()
        client = _FakeDeleteClient(failures=2)
        monkeypatch.setattr(mod, "get_client", lambda: client)

        result = _run(mod.delete_flow_run_id.fn(flow_run_id))

        assert result == flow_run_id
        assert client.calls == 3

    def test_raises_after_max_attempts_on_persistent_503(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flow_run_id = uuid.uuid4()
        client = _FakeDeleteClient(failures=mod._DELETE_ATTEMPTS)
        monkeypatch.setattr(mod, "get_client", lambda: client)

        with pytest.raises(PrefectHTTPStatusError):
            _run(mod.delete_flow_run_id.fn(flow_run_id))

        assert client.calls == mod._DELETE_ATTEMPTS

    def test_non_retryable_error_is_not_retried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flow_run_id = uuid.uuid4()
        client = _FakeDeleteClient(failures=1, error=ValueError("boom"))
        monkeypatch.setattr(mod, "get_client", lambda: client)

        with pytest.raises(ValueError, match="boom"):
            _run(mod.delete_flow_run_id.fn(flow_run_id))

        assert client.calls == 1

    def test_4xx_http_error_is_not_retried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flow_run_id = uuid.uuid4()
        request = httpx.Request("DELETE", "http://127.0.0.1:4200/api/flow_runs/x")
        response = httpx.Response(404, request=request)
        error = PrefectHTTPStatusError("Not Found", request=request, response=response)
        client = _FakeDeleteClient(failures=1, error=error)
        monkeypatch.setattr(mod, "get_client", lambda: client)

        with pytest.raises(PrefectHTTPStatusError):
            _run(mod.delete_flow_run_id.fn(flow_run_id))

        assert client.calls == 1
