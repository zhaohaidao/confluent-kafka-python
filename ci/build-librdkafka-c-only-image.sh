#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." >/dev/null && pwd)"

LIBRDKAFKA_REPO="${LIBRDKAFKA_REPO:-/data/kafka/librdkafka/.worktrees/pr-split}"
LIBRDKAFKA_REF="${LIBRDKAFKA_REF:-HEAD}"
BASE_IMAGE="${BASE_IMAGE:-quay.io/pypa/manylinux_2_28_x86_64:latest}"
BUILDER_IMAGE="${BUILDER_IMAGE:-quay.io/pypa/manylinux_2_28_x86_64:latest}"
INSTALL_OS_DEPS="${INSTALL_OS_DEPS:-1}"

commit="$(git -C "${LIBRDKAFKA_REPO}" rev-parse "${LIBRDKAFKA_REF}^{commit}")"
short_commit="$(git -C "${LIBRDKAFKA_REPO}" rev-parse --short "${commit}")"
IMAGE_TAG="${IMAGE_TAG:-red-kafka-python-librdkafka:${short_commit}-manylinux_2_28}"

tmpdir="$(mktemp -d)"
cleanup() {
    rm -rf "${tmpdir}"
}
trap cleanup EXIT

git -C "${LIBRDKAFKA_REPO}" archive --format=tar --output="${tmpdir}/librdkafka-src.tar" "${commit}"

docker build \
    --build-arg "BASE_IMAGE=${BASE_IMAGE}" \
    --build-arg "BUILDER_IMAGE=${BUILDER_IMAGE}" \
    --build-arg "LIBRDKAFKA_COMMIT=${commit}" \
    --build-arg "LIBRDKAFKA_PREFIX=/opt/librdkafka" \
    --build-arg "INSTALL_OS_DEPS=${INSTALL_OS_DEPS}" \
    --build-arg "http_proxy=${http_proxy:-}" \
    --build-arg "https_proxy=${https_proxy:-}" \
    --build-arg "HTTP_PROXY=${HTTP_PROXY:-${http_proxy:-}}" \
    --build-arg "HTTPS_PROXY=${HTTPS_PROXY:-${https_proxy:-}}" \
    --build-arg "no_proxy=${no_proxy:-}" \
    --build-arg "NO_PROXY=${NO_PROXY:-${no_proxy:-}}" \
    -f "${REPO_ROOT}/ci/librdkafka-c-only.Dockerfile" \
    -t "${IMAGE_TAG}" \
    "${tmpdir}"

printf 'Built image: %s\n' "${IMAGE_TAG}"
printf 'librdkafka commit: %s\n' "${commit}"

if [[ -n "${IMAGE_TAG_FILE:-}" ]]; then
    printf '%s\n' "${IMAGE_TAG}" >"${IMAGE_TAG_FILE}"
fi
