"""Conditional Prefect decorators.

When the environment variable ``PREFECT_ENABLED`` is set to a truthy value
(``1``, ``true``, ``yes`` — case-insensitive), the :func:`task` and
:func:`flow` decorators behave exactly like ``prefect.task`` and
``prefect.flow``.  Otherwise they are transparent no-ops that return the
decorated function unchanged.

Usage::

    from irsol_data_pipeline.orchestration.decorators import task, flow

    @task(name="my-task", retries=2)
    def do_work():
        ...

    @flow(name="my-flow")
    def run_pipeline():
        ...
"""

from __future__ import annotations

import os
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar, overload

P = ParamSpec("P")
R = TypeVar("R")

_TRUTHY = {"1", "true", "yes"}


def prefect_enabled() -> bool:
    """Return ``True`` when Prefect orchestration is activated."""
    return os.environ.get("PREFECT_ENABLED", "").strip().lower() in _TRUTHY


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


@overload
def task(fn: Callable[P, R]) -> Callable[P, R]: ...
@overload
def task(
    fn: None = None, **kwargs: Any
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def task(
    fn: Callable[P, R] | None = None,
    **kwargs: Any,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Conditionally apply ``prefect.task``.

    Acts as ``prefect.task`` when *PREFECT_ENABLED* is truthy, otherwise a
    transparent no-op.  Supports both ``@task`` and ``@task(...)`` forms.
    """
    if prefect_enabled():
        from prefect import task as prefect_task

        return prefect_task(fn, **kwargs)  # type: ignore[return-value]
    if fn is not None:
        return fn

    def wrapper(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def inner(*args: P.args, **kw: P.kwargs) -> R:
            return func(*args, **kw)

        return inner

    return wrapper


@overload
def flow(fn: Callable[P, R]) -> Callable[P, R]: ...
@overload
def flow(
    fn: None = None, **kwargs: Any
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def flow(
    fn: Callable[P, R] | None = None,
    **kwargs: Any,
) -> Callable[P, R] | Callable[[Callable[P, R]], Callable[P, R]]:
    """Conditionally apply ``prefect.flow``.

    Acts as ``prefect.flow`` when *PREFECT_ENABLED* is truthy, otherwise a
    transparent no-op.  Supports both ``@flow`` and ``@flow(...)`` forms.
    """
    if prefect_enabled():
        from prefect import flow as prefect_flow

        return prefect_flow(fn, **kwargs)  # type: ignore[return-value]
    if fn is not None:
        return fn

    def wrapper(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def inner(*args: P.args, **kw: P.kwargs) -> R:
            return func(*args, **kw)

        return inner

    return wrapper
