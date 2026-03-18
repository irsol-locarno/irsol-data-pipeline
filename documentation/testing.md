# Testing

## Running tests

Run the full test suite with coverage:

```bash
make test
```

This runs `pytest` with coverage reports in HTML (`htmlcov/`) and XML (`coverage.xml`).

To run a single test file:

```bash
uv run pytest tests/unit/test_scanner.py -v
```

To run tests matching a keyword:

```bash
uv run pytest -k "flatfield" -v
```

## Test layout

Tests live in `tests/unit/`, one file per source module (`test_<module>.py`).

## Conventions

- Group related tests in classes: `class TestFoo:`.
- Use `@pytest.mark.parametrize` for data-driven tests.
- Use `unittest.mock.patch` / `patch.dict` for mocking; use the built-in `tmp_path` fixture for temporary files.
- Tests must not read real `.dat` files from disk — construct in-memory `numpy` data instead.
