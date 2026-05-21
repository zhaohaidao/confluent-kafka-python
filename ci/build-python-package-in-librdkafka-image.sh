#!/usr/bin/env bash
set -eo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"

cd "${repo_root}"

export CFLAGS="-I/opt/librdkafka/include"
export LDFLAGS="-L/opt/librdkafka/lib"
export LD_LIBRARY_PATH="/opt/librdkafka/lib:${LD_LIBRARY_PATH:-}"

for py in /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python; do
    "${py}" -m pip config set global.index-url http://pypi.devops.xiaohongshu.com/simple
    "${py}" -m pip config set global.trusted-host pypi.devops.xiaohongshu.com
done

rm -rf build dist wheelhouse red_kafka.egg-info
mkdir -p dist wheelhouse

for py in /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python; do
    "${py}" -m pip install --no-cache-dir setuptools wheel
    rm -rf build red_kafka.egg-info /tmp/red-kafka-wheel
    mkdir -p /tmp/red-kafka-wheel
    "${py}" -m pip wheel . --no-deps -w /tmp/red-kafka-wheel
    auditwheel repair /tmp/red-kafka-wheel/red_kafka-*.whl -w wheelhouse
done

rm -rf build red_kafka.egg-info
/opt/python/cp311-cp311/bin/python setup.py sdist -d dist

idx=0
for py in /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python; do
    idx=$((idx + 1))
    venv="/tmp/red-kafka-import-${idx}"
    "${py}" -m venv "${venv}"
    . "${venv}/bin/activate"
    python -m pip install --no-cache-dir "${repo_root}"
    cd /tmp
    python - <<'PY'
import confluent_kafka as ck

print("red-kafka:", ck.version())
print("librdkafka:", ck.libversion())
PY
    cd "${repo_root}"
    deactivate
    rm -rf build red_kafka.egg-info "${venv}"
done

find dist wheelhouse -maxdepth 1 -type f -printf "%f\n" | sort
sha256sum dist/* wheelhouse/*
