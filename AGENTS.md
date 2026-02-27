# Repository Guidelines

## Project Structure & Module Organization
The core Python package lives in `confluent_kafka/`, which contains the public API and C-extension bindings to librdkafka. Tests are under `tests/` with unit tests in top-level modules and integration coverage in `tests/integration/`. Integration orchestration and cluster scripts live in `tests/docker/` and are driven by `tests/run.sh`. Documentation sources are in `docs/` with generated output written to `docs/_build/`. Example apps are in `examples/`. Build artifacts often appear in `build/` and `wheelhouse/`.

## Build, Test, and Development Commands
- `python setup.py build` builds the extension and package locally.
- `C_INCLUDE_PATH=/path/to/include LIBRARY_PATH=/path/to/lib python setup.py build` points to a non-standard librdkafka install.
- `make docs` or `python setup.py build_sphinx` generates Sphinx HTML in `docs/_build/`.
- `./tests/run.sh unit` runs unit tests for the active interpreter.
- `./tests/run.sh tox` runs unit tests across tox envs (requires `pip install tox`).
- `./tests/run.sh all` runs unit + integration tests with the current interpreter.
- `tox -e flake8` runs linting only.

## Coding Style & Naming Conventions
Python code follows 4-space indentation and PEP 8 naming (`snake_case` modules/functions, `CapWords` classes). Linting is enforced via `flake8` with `max-line-length = 119` (see `tox.ini`). Test files use `test_*.py` and should keep fixtures/helpers near related tests.

## Testing Guidelines
The test runner is `pytest`, with `tox` providing multi-interpreter coverage. Integration tests require Docker Engine and docker-compose; the default runner uses `tests/run.sh` and expects a `testconf.json` as the final argument when customizing cluster configuration (see `tests/testconf-example.json`). By default, `pytest` ignores `tests/integration/`, so use `tests/run.sh` for full coverage. To target specific integration modes: `./tests/run.sh --producer --consumer`.

## Commit & Pull Request Guidelines
Recent history favors short, direct commit summaries without prefixes. Keep commit subjects concise and action-oriented (e.g., "fix admin timeout"). PRs should include a clear description of behavior changes, linked issues if applicable, and a short test note (commands run and scope). Include doc updates when public APIs or behavior change.

## Configuration Tips
This project depends on librdkafka; if your system install is non-standard, export `C_INCLUDE_PATH` and `LIBRARY_PATH` as shown above. Integration tests spin up local Kafka via Docker, so verify the Docker daemon is running before invoking `tests/run.sh`.

## Environment Notes
- Before running any command in this workspace, activate `/home/admin/mh/fluss-r/.venv/bin/activate` to ensure the correct Python environment is used.
- If pytest/import fails with librdkafka dynamic linking errors (for example `librdkafka.so => not found` or `undefined symbol: rd_kafka_zstd_compress`), export `LD_LIBRARY_PATH=/home/admin/mh/kafka/librdkafka/src:$LD_LIBRARY_PATH` before running test/build commands.
