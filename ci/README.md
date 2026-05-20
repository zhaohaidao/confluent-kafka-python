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
  `quay.io/pypa/manylinux_2_28_x86_64:latest`. The Python SDK release wheel
  targets glibc 2.28 and does not support the older
  `manylinux2014` / `manylinux_2_17` baseline.
- `BUILDER_IMAGE`: build-stage image. Defaults to the same
  `quay.io/pypa/manylinux_2_28_x86_64:latest` image so the compiled
  `librdkafka` ABI matches the final image and the release wheel.
- `IMAGE_TAG`: output image tag.
- `INSTALL_OS_DEPS`: set to `1` only for bare custom images that do not already
  provide compiler, CMake, Python 3.11+, zlib, zstd, and OpenSSL development
  files. This defaults to `1` for the builder stage.

The build script forwards `http_proxy`, `https_proxy`, `HTTP_PROXY`,
`HTTPS_PROXY`, `no_proxy`, and `NO_PROXY` into `docker build` when they are set
on the host. Use these variables when the manylinux image needs proxy access to
install OS packages.

The image build fails if `librdkafka` exports bundled `tinycthread` symbols.
The expected thread symbol is:

```text
U thrd_create@GLIBC_2.28
```

## Run core Python SDK tests

```bash
IMAGE_TAG=red-kafka-python-librdkafka:280be6f \
ci/run-core-unit-tests-in-librdkafka-image.sh
```

To verify a release-candidate package version, pass
`RED_KAFKA_PACKAGE_VERSION` through the runner:

```bash
RED_KAFKA_PACKAGE_VERSION=0.1rc15 \
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
