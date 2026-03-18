# Project Guidelines

## Overview

`irsol-data-pipeline` processes ZIMPOL spectropolarimetric solar observations from IRSOL.
It reads raw `.dat`/`.sav` IDL files, applies flat-field and smile corrections, performs
wavelength auto-calibration, and exports corrected Stokes (I, Q, U, V) data as FITS files.

Key domain terms: **Stokes parameters**, **flat-field correction**, **smile correction**,
**wavelength auto-calibration**, **ZIMPOL**, **observation day**.

## Tools at your disposal

* Run all unit tests with: `make test`
* Run linters and formatters with: `make lint`

## Code Style

- **Python ≥ 3.10**; always add `from __future__ import annotations` as the first import in every source file.
- **Formatter/linter**: managed by `make lint` command.
- **Type hints**: exhaustive on all function arguments and return types. Use modern syntax — `T | None` and `A | B` — not `Optional[T]` or `Union[A, B]`.
- **Naming**: `snake_case` for variables/functions/files, `PascalCase` for classes, `UPPER_SNAKE_CASE` for module-level constants, `_leading_underscore` for private helpers.
- **Docstrings**: Google style with `Args:`, `Returns:`, `Raises:` sections. Every public function and class requires a docstring. Every module requires a module-level docstring.

## Architecture

```
src/irsol_data_pipeline/
├── core/          # Domain models (Pydantic), config constants, calibration, correction
├── io/            # Importers and exporters (dat, fits, flatfield, processing_metadata)
├── pipeline/      # High-level processing logic (scanner, day_processor, measurement_processor)
├── orchestration/ # Prefect flows/tasks, conditional decorators, retry helpers, logging bridge
└── plotting/      # Matplotlib-based Stokes profile plots
```

## Conventions

### Logging
Use `loguru`. Always pass context as **keyword arguments** — never use f-string interpolation inside log calls:
```python
# correct
logger.info("Processing measurement", file=path.name, day=day)
# wrong
logger.info(f"Processing measurement {path.name}")
```
Use `logger.contextualize(...)` as a context manager to bind request-scoped fields.
Use `logger.exception(...)` inside `except` blocks to capture tracebacks.

### Exceptions
All custom exceptions live in `irsol_data_pipeline.exceptions` and inherit from `IrsolDataPipelineException`.
Always raise the most specific domain exception (`FitsImportError`, `DatImportError`, `FlatFieldAssociationNotFoundException`, etc.) rather than a bare built-in.

### Pydantic Models
- Use Pydantic v2 (`BaseModel`, `ConfigDict`, `field_validator`).
- Apply `model_config = ConfigDict(frozen=True)` for immutable value objects.
- Use `arbitrary_types_allowed=True` when a model holds `numpy.ndarray` or `spectroflat` objects.
- Use `@field_validator(..., mode="before")` for input coercions (e.g. datetime strings, yes/no booleans).

### Prefect Tasks and Flows
Use the project's **conditional decorators** from `irsol_data_pipeline.orchestration.decorators`, not directly from `prefect`:
```python
from irsol_data_pipeline.orchestration.decorators import flow, task

@task(retries=2, retry_delay_seconds=10)
def my_task(): ...
```
These are transparent no-ops when `PREFECT_ENABLED` is not set, allowing the pipeline to run as plain Python.

You're only supposed to use and import `prefect` only within the `orchestration/` module. No `prefect` imports or decorators should be used in `core/`, `io/`, `pipeline/`, or `plotting/`.

### IO Importers / Exporters
Each `io/<format>/importer.py` and `io/<format>/exporter.py` must raise its corresponding typed exception on failure (`FitsImportError`, `DatImportError`, `FlatfieldCorrectionImportError`, etc.).

## Testing

- Framework: **pytest** only (no `unittest`).
- Group related tests in classes: `class TestFoo:`.
- Use `@pytest.mark.parametrize` for data-driven tests.
- Use `unittest.mock.patch` / `patch.dict` for mocking; use the built-in `tmp_path` fixture for temporary files.
- Tests must not read real `.dat` files from disk — construct in-memory `numpy` data instead.
- Test files: `tests/unit/test_<module>.py`.
