#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." >/dev/null && pwd)"

IMAGE_TAG="${IMAGE_TAG:-red-kafka-python-librdkafka:latest}"
RUN_FLAKE8="${RUN_FLAKE8:-0}"
INSTALL_DEV_EXTRAS="${INSTALL_DEV_EXTRAS:-0}"
PIP_INDEX_URL="${PIP_INDEX_URL:-http://mirrors.tencentyun.com/pypi/simple}"
PIP_TRUSTED_HOST="${PIP_TRUSTED_HOST:-mirrors.tencentyun.com}"

if [[ "$#" -gt 0 ]]; then
    PYTEST_TARGETS="$*"
else
    PYTEST_TARGETS="${PYTEST_TARGETS:-tests/test_red_eds.py tests/test_red_metrics.py tests/test_public_clients.py tests/test_auth_csv_runner_tool.py tests/test_Producer.py tests/test_Consumer.py tests/test_Admin.py tests/test_misc.py}"
fi

docker run --rm \
    -e "RUN_FLAKE8=${RUN_FLAKE8}" \
    -e "INSTALL_DEV_EXTRAS=${INSTALL_DEV_EXTRAS}" \
    -e "PIP_INDEX_URL=${PIP_INDEX_URL}" \
    -e "PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}" \
    -e "PYTEST_TARGETS=${PYTEST_TARGETS}" \
    -v "${REPO_ROOT}:/workspace" \
    -w /workspace \
    "${IMAGE_TAG}" \
    sh -lc '
        set -eu

        if [ -z "${PYTHON_BIN:-}" ]; then
            if command -v python3.11 >/dev/null 2>&1; then
                PYTHON_BIN=python3.11
            elif [ -x /opt/python/cp311-cp311/bin/python ]; then
                PYTHON_BIN=/opt/python/cp311-cp311/bin/python
            else
                PYTHON_BIN=python3
            fi
        fi
        "${PYTHON_BIN}" - <<'"'"'PY'"'"'
import sys

if sys.version_info < (3, 11):
    raise SystemExit("Python >= 3.11 is required, got %s" % (sys.version,))
PY
        "${PYTHON_BIN}" -m venv /tmp/red-kafka-python-venv
        . /tmp/red-kafka-python-venv/bin/activate

        python -m pip config set global.index-url "${PIP_INDEX_URL}"
        python -m pip config set global.trusted-host "${PIP_TRUSTED_HOST}"
        python -m pip install -U pip setuptools wheel
        if [ "${INSTALL_DEV_EXTRAS}" = "1" ]; then
            python -m pip install -e ".[dev]"
        else
            python -m pip install -e .
            python -m pip install pytest pytest-timeout
            if [ "${RUN_FLAKE8}" = "1" ]; then
                python -m pip install flake8
            fi
        fi

        python - <<'"'"'PY'"'"'
import confluent_kafka as ck

print("red-kafka:", ck.version())
print("librdkafka:", ck.libversion())
PY

        if [ "${RUN_FLAKE8}" = "1" ]; then
            python -m flake8
        fi

        python -m pytest -q ${PYTEST_TARGETS}
    '
