# Python SDK CI with C-only librdkafka

This CI path builds a Docker image that contains a C-only `librdkafka`
installation, then runs Python SDK unit tests inside that image.

The `librdkafka` build follows the CMake C-only mode documented in
`/data/kafka/librdkafka/.worktrees/pr-split`:

```bash
cmake -S . -B _cmake_build_c \
  -DRDKAFKA_BUILD_CPP=OFF \
  -DBUILD_SHARED_LIBS=ON
cmake --build _cmake_build_c --target rdkafka
cmake --install _cmake_build_c --prefix /opt/librdkafka
```

## Build the test image

```bash
LIBRDKAFKA_REPO=/data/kafka/librdkafka/.worktrees/pr-split \
LIBRDKAFKA_REF=280be6f \
IMAGE_TAG=red-kafka-python-librdkafka:280be6f \
ci/build-librdkafka-c-only-image.sh
```

Useful variables:

- `LIBRDKAFKA_REPO`: local `librdkafka` git repository or worktree.
- `LIBRDKAFKA_REF`: commit, branch, or tag to archive into the image.
- `BASE_IMAGE`: build image base. Defaults to
  `quay.io/pypa/manylinux2014_x86_64:latest` because the Python SDK requires
  Python 3.11+ and manylinux2014 provides CPython 3.11 under
  `/opt/python/cp311-cp311/bin/python`.
- `BUILDER_IMAGE`: build-stage image. Defaults to
  `docker-reg.devops.xiaohongshu.com/cpp-infra/build_env:brpc_v20250728`,
  matching the C-only build environment documented in the `librdkafka`
  `pr-split` worktree. Keep `BUILDER_IMAGE` and `BASE_IMAGE` ABI-compatible
  because the final image runs the shared `librdkafka` built in the builder
  stage.
- `IMAGE_TAG`: output image tag.
- `INSTALL_OS_DEPS`: set to `1` only for bare custom images that do not already
  provide compiler, CMake, Python 3.11+, zlib, zstd, and OpenSSL development
  files. This defaults to `1` for the builder stage.

## Run core Python SDK tests

```bash
IMAGE_TAG=red-kafka-python-librdkafka:280be6f \
ci/run-core-unit-tests-in-librdkafka-image.sh
```

The default test set is the fast core suite:

```text
tests/test_red_eds.py
tests/test_red_metrics.py
tests/test_public_clients.py
tests/test_auth_csv_runner_tool.py
tests/test_Producer.py
tests/test_Consumer.py
tests/test_Admin.py
tests/test_misc.py
```

To override the test set:

```bash
IMAGE_TAG=red-kafka-python-librdkafka:280be6f \
ci/run-core-unit-tests-in-librdkafka-image.sh tests/test_red_eds.py
```

The runner skips lint by default because current unpinned `flake8` reports
existing style issues in this legacy codebase. Set `RUN_FLAKE8=1` only for a
lint CI job with an agreed flake8 version/policy:

```bash
RUN_FLAKE8=1 IMAGE_TAG=red-kafka-python-librdkafka:280be6f \
ci/run-core-unit-tests-in-librdkafka-image.sh
```

The core test runner installs only the package runtime dependencies plus
`pytest` and `pytest-timeout`. It installs `flake8` only when
`RUN_FLAKE8=1`. Set `INSTALL_DEV_EXTRAS=1` if a CI job needs the full `.[dev]`
dependency set, including Avro dependencies.
