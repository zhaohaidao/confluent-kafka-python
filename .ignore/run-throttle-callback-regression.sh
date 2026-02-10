#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null && pwd )"
DOCKER_BIN="${REPO_ROOT}/tests/docker/bin"
DOCKER_CONTEXT="${REPO_ROOT}/tests/docker/docker-compose.yaml"

VENV_ACTIVATE="${VENV_ACTIVATE:-/home/admin/mh/fluss-r/.venv/bin/activate}"
if [[ -f "${VENV_ACTIVATE}" ]]; then
    # shellcheck disable=SC1090
    source "${VENV_ACTIVATE}"
fi

TOPIC="${TEST_THROTTLE_TOPIC:-test-throttle-callback}"
BOOTSTRAP_SERVERS="${TEST_THROTTLE_BOOTSTRAP_SERVERS:-localhost:29092}"
AUTO_DOWN="${AUTO_DOWN:-1}"

LIBRDKAFKA_INCLUDE="${LIBRDKAFKA_INCLUDE:-/home/admin/mh/kafka/_cmake_install/include}"
LIBRDKAFKA_LIB="${LIBRDKAFKA_LIB:-/home/admin/mh/kafka/_cmake_install/lib}"
BUILD_EXTENSION="${BUILD_EXTENSION:-1}"

cleanup() {
    if [[ "${AUTO_DOWN}" == "1" ]]; then
        "${DOCKER_BIN}/cluster_down.sh"
    fi
}

trap cleanup EXIT

echo "[1/5] Bringing up docker test cluster..."
"${DOCKER_BIN}/cluster_up.sh"

echo "[2/5] Ensuring throttle client quota is configured..."
docker-compose -f "${DOCKER_CONTEXT}" exec -T kafka sh -c "
    /usr/bin/kafka-configs --zookeeper zookeeper:2181 \
      --alter --add-config 'producer_byte_rate=1,consumer_byte_rate=1,request_percentage=001' \
      --entity-name throttled_client --entity-type clients
"

echo "[3/5] Ensuring test topic exists..."
docker-compose -f "${DOCKER_CONTEXT}" exec -T kafka sh -c "
    /usr/bin/kafka-topics --zookeeper zookeeper:2181 \
      --create --if-not-exists --topic ${TOPIC} --partitions 1 --replication-factor 1
"

if [[ "${BUILD_EXTENSION}" == "1" ]]; then
    echo "[4/5] Building extension in-place..."
    C_INCLUDE_PATH="${LIBRDKAFKA_INCLUDE}" \
    LIBRARY_PATH="${LIBRDKAFKA_LIB}" \
    python setup.py build_ext --inplace
fi

echo "[5/5] Running throttle callback propagation test..."
export TEST_THROTTLE_BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS}"
export TEST_THROTTLE_TOPIC="${TOPIC}"
export LD_LIBRARY_PATH="${LIBRDKAFKA_LIB}:${LD_LIBRARY_PATH:-}"

cd "${REPO_ROOT}"
pytest -q -rs tests/test_misc.py::test_throttle_cb_exception_is_propagated

echo "Done."
