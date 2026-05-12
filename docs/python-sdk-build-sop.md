# Python SDK Build SOP

This SOP describes how to build, validate, and package the `red-kafka` Python
SDK from this repository. Commands are intended to run from the repository root.

## Scope

- Build the local C extension against `librdkafka`.
- Run the minimum validation before publishing or handing off artifacts.
- Produce source and wheel artifacts without relying on machine-specific paths.

## Prerequisites

- Python 3.11 or newer.
- A working C compiler and Python development headers.
- `pip`, `setuptools`, and `wheel`.
- `librdkafka` headers and shared library.

If `librdkafka` is installed in a non-standard prefix, export the prefix through
an environment variable and derive all paths from it:

```bash
export LIBRDKAFKA_PREFIX=/path/to/librdkafka-prefix
export C_INCLUDE_PATH="$LIBRDKAFKA_PREFIX/include${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
export LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

On macOS, use `DYLD_LIBRARY_PATH` instead of `LD_LIBRARY_PATH` when a runtime
loader path is required.

## Environment Setup

Use an isolated virtual environment:

```bash
python3.11 -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

`$VENV` should be set by the operator, for example to a path under the current
workspace or another local scratch directory. Do not commit virtualenvs or build
outputs.

## Local Build

Build the extension in place:

```bash
python setup.py build
```

If `librdkafka` is not installed in the system default include/library paths,
keep `C_INCLUDE_PATH`, `LIBRARY_PATH`, and the runtime loader path exported as
shown above.

Verify the built extension can import and reports the linked library version:

```bash
python - <<'PY'
import confluent_kafka

print("python package:", confluent_kafka.version())
print("librdkafka:", confluent_kafka.libversion())
PY
```

## Validation

Run lint and unit tests:

```bash
python -m flake8
python -m pytest -q
```

If `tox` is installed and the required interpreters are available, run the tox
matrix:

```bash
tox
```

For targeted RED runtime changes, run the focused tests first:

```bash
python -m pytest -q tests/test_red_eds.py tests/test_red_metrics.py tests/test_public_clients.py
```

Integration tests require Docker and a Kafka test configuration. Use the project
test runner when those dependencies are available:

```bash
./tests/run.sh unit
./tests/run.sh all
```

## Package Artifacts

Set the release version explicitly when producing distributable artifacts:

```bash
export RED_KAFKA_PACKAGE_VERSION=<version>
rm -rf build dist wheelhouse red_kafka.egg-info
python setup.py sdist bdist_wheel
python -m pip wheel . --no-deps --wheel-dir wheelhouse
```

Expected outputs:

- `dist/red-kafka-<version>.tar.gz`
- `dist/red_kafka-<version>-*.whl`
- `wheelhouse/red_kafka-<version>-*.whl`

Do not commit `build/`, `dist/`, `wheelhouse/`, or `*.egg-info/`.

## Artifact Smoke Test

Install the wheel into a fresh virtual environment and run an import smoke test:

```bash
python3.11 -m venv "$SMOKE_VENV"
. "$SMOKE_VENV/bin/activate"
python -m pip install --upgrade pip
python -m pip install dist/red_kafka-<version>-*.whl
python - <<'PY'
from confluent_kafka import Consumer, Producer, libversion, version

print("python package:", version())
print("librdkafka:", libversion())
Producer({"bootstrap.servers": "127.0.0.1:1"}).flush(0)
Consumer({
    "bootstrap.servers": "127.0.0.1:1",
    "group.id": "smoke-test",
}).close()
PY
```

If the smoke test fails with a dynamic linker error, confirm that the runtime
loader path points to the same `librdkafka` prefix used during build.

## Release Checklist

- Confirm `RED_KAFKA_PACKAGE_VERSION` matches the intended release version.
- Confirm `python -m flake8` and relevant `pytest` suites passed.
- Confirm the wheel smoke test passed in a fresh virtual environment.
- Confirm generated artifacts are not tracked by Git unless explicitly required
  by the release process.
- Record the `librdkafka` version used for the build in the release notes or PR
  description.
