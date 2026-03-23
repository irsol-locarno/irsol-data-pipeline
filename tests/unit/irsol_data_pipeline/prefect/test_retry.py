"""Tests for retry condition helpers used by Prefect tasks."""

from irsol_data_pipeline.prefect.retry import (
    retry_condition_except_on_exceptions,
)


class _SuccessfulState:
    def result(self) -> None:
        return None


class _FailingState:
    def __init__(self, exc: BaseException) -> None:
        self._exc = exc

    def result(self) -> None:
        raise self._exc


class BaseCustomError(Exception):
    pass


class ChildCustomError(BaseCustomError):
    pass


def test_retry_handler_returns_true_when_state_succeeds() -> None:
    handler = retry_condition_except_on_exceptions(ValueError)

    should_retry = handler(None, None, _SuccessfulState())

    assert should_retry is True


def test_retry_handler_returns_false_for_configured_exception() -> None:
    handler = retry_condition_except_on_exceptions(ValueError)

    should_retry = handler(None, None, _FailingState(ValueError("boom")))

    assert should_retry is False


def test_retry_handler_returns_false_for_configured_exception_subclass() -> None:
    handler = retry_condition_except_on_exceptions(BaseCustomError)

    should_retry = handler(None, None, _FailingState(ChildCustomError("boom")))

    assert should_retry is False


def test_retry_handler_returns_true_for_unconfigured_exception() -> None:
    handler = retry_condition_except_on_exceptions(ValueError)

    should_retry = handler(None, None, _FailingState(RuntimeError("boom")))

    assert should_retry is True


def test_retry_handler_checks_all_configured_exception_types() -> None:
    handler = retry_condition_except_on_exceptions(ValueError, KeyError, RuntimeError)

    should_retry = handler(None, None, _FailingState(KeyError("boom")))

    assert should_retry is False


def test_retry_handler_retries_when_no_exception_types_configured() -> None:
    handler = retry_condition_except_on_exceptions()

    should_retry = handler(None, None, _FailingState(ValueError("boom")))

    assert should_retry is True
