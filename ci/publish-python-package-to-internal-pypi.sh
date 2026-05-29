#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
    echo "Usage: $0 <package-version>" >&2
    exit 2
fi

package_version="$1"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
python_bin="/opt/python/cp311-cp311/bin/python"

cd "${repo_root}"

if [[ -n "${TWINE_USERNAME:-}" && -n "${TWINE_PASSWORD:-}" ]]; then
    export TWINE_REPOSITORY_URL="${TWINE_REPOSITORY_URL:-http://pypi.devops.xiaohongshu.com}"
    twine_upload_args=(--repository-url "${TWINE_REPOSITORY_URL}")
elif [[ -f "${HOME}/.pypirc" ]] && grep -q '^\[red\]' "${HOME}/.pypirc"; then
    twine_upload_args=(-r red)
else
    echo "Missing publish credentials. Set TWINE_USERNAME/TWINE_PASSWORD in CI, or provide ${HOME}/.pypirc with [red]." >&2
    exit 1
fi

export RED_KAFKA_PACKAGE_VERSION="${package_version}"
"${script_dir}/build-python-package-in-librdkafka-image.sh"

"${python_bin}" -m pip config set global.index-url http://pypi.devops.xiaohongshu.com/simple
"${python_bin}" -m pip config set global.trusted-host pypi.devops.xiaohongshu.com
"${python_bin}" -m pip install --no-cache-dir twine
"${python_bin}" -m twine check dist/* wheelhouse/*.whl

if "${python_bin}" -m pip index versions red-kafka --pre \
    -i http://pypi.devops.xiaohongshu.com/simple/ \
    --trusted-host pypi.devops.xiaohongshu.com 2>/dev/null |
    grep -Eq "(^|[,[:space:]])${package_version//./\\.}([,[:space:]]|$)"; then
    echo "red-kafka ${package_version} already exists in internal PyPI; skipping upload."
else
    export TWINE_NON_INTERACTIVE=1
    "${python_bin}" -m twine upload "${twine_upload_args[@]}" dist/* wheelhouse/*.whl
fi

venv="/tmp/red-kafka-upload-check-${package_version}"
rm -rf "${venv}"
"${python_bin}" -m venv "${venv}"
. "${venv}/bin/activate"
python -m pip config set global.index-url http://pypi.devops.xiaohongshu.com/simple
python -m pip config set global.trusted-host pypi.devops.xiaohongshu.com
python -m pip install --no-cache-dir --force-reinstall "red-kafka==${package_version}"
cd /tmp
python - "${package_version}" <<'PY'
import importlib.metadata as metadata
import sys
import confluent_kafka as ck

expected = sys.argv[1]
installed = metadata.version("red-kafka")
if installed != expected:
    raise SystemExit("installed red-kafka version %r, expected %r" % (installed, expected))

print("metadata:", installed)
print("red-kafka:", ck.version())
print("librdkafka:", ck.libversion())
PY
deactivate
rm -rf "${venv}"
