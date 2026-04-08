# Red Kafka Private PyPI Packaging SOP (2026-04-08)

## 1. Scope

This SOP is for rebuilding and publishing `red-kafka` RC packages to private PyPI with explicit environment variables.

## 2. Prerequisites

- Build host has Python `3.11+`
- `librdkafka` headers and shared libs are available at:
  - `/home/admin/mh/kafka/librdkafka/.install-auth-redfetch-current/include`
  - `/home/admin/mh/kafka/librdkafka/.install-auth-redfetch-current/lib`
- Private PyPI endpoint is reachable:
  - `http://pypi.devops.xiaohongshu.com/simple/`

## 3. Environment Variables

```bash
cd /home/admin/mh/kafka/confluent-kafka-python
source /home/admin/mh/fluss-r/.venv/bin/activate

# Required: package version
export RED_KAFKA_PACKAGE_VERSION=0.1rc7

# Required: build/link librdkafka
export C_INCLUDE_PATH=/home/admin/mh/kafka/librdkafka/.install-auth-redfetch-current/include
export LIBRARY_PATH=/home/admin/mh/kafka/librdkafka/.install-auth-redfetch-current/lib
export LD_LIBRARY_PATH=/home/admin/mh/kafka/librdkafka/.install-auth-redfetch-current/lib:$LD_LIBRARY_PATH

# Optional: network proxy
# export https_proxy=http://10.3.4.34:3128
# export http_proxy=http://10.3.4.34:3128
```

## 4. Clean Workspace

```bash
rm -rf build dist wheelhouse *.egg-info red_kafka.egg-info
find . -type d -name "__pycache__" -prune -exec rm -rf {} +
find . -type f -name "*.pyc" -delete
```

## 5. Build Artifacts

```bash
python -m pip install -U pip setuptools wheel build auditwheel twine
python setup.py sdist bdist_wheel
```

Expected outputs:

- `dist/red_kafka-${RED_KAFKA_PACKAGE_VERSION}.tar.gz`
- `dist/red_kafka-${RED_KAFKA_PACKAGE_VERSION}-cp311-cp311-linux_x86_64.whl`

## 6. Build Self-Contained Wheel

```bash
mkdir -p wheelhouse
auditwheel repair \
  dist/red_kafka-${RED_KAFKA_PACKAGE_VERSION}-cp311-cp311-linux_x86_64.whl \
  -w wheelhouse
```

Expected output:

- `wheelhouse/red_kafka-${RED_KAFKA_PACKAGE_VERSION}-cp311-cp311-manylinux_*.whl`

## 7. Validate Before Upload

### 7.1 Metadata Check

```bash
tar -xOf dist/red_kafka-${RED_KAFKA_PACKAGE_VERSION}.tar.gz \
  red_kafka-${RED_KAFKA_PACKAGE_VERSION}/PKG-INFO \
  | head -n 12
```

Required checks:

- `Version: ${RED_KAFKA_PACKAGE_VERSION}`
- `Requires-Python: >=3.11`

### 7.2 Install Check in Clean Venv

```bash
python -m venv /tmp/venv_red_kafka_pkg_check
source /tmp/venv_red_kafka_pkg_check/bin/activate

pip install --no-cache-dir wheelhouse/*.whl
python -c "import confluent_kafka as ck; print(ck.version())"
```

The printed version should start with `${RED_KAFKA_PACKAGE_VERSION}`.

## 8. Upload to Private PyPI

If `~/.pypirc` has repository alias `red`:

```bash
twine upload -r red \
  dist/red_kafka-${RED_KAFKA_PACKAGE_VERSION}.tar.gz \
  wheelhouse/*.whl
```

## 9. Consumer Install Verification

```bash
python -m venv /tmp/venv_red_kafka_consumer_check
source /tmp/venv_red_kafka_consumer_check/bin/activate

pip install --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==${RED_KAFKA_PACKAGE_VERSION}

python -c "import confluent_kafka as ck; print(ck.version())"
```

## 10. Troubleshooting

### 10.1 `expected '0.1rcX', but metadata has '1.3.0'`

Root cause:

- `pip` fell back to `sdist`, and package metadata/version in source build path was inconsistent.

Fix:

- Ensure this build uses clean workspace and sets `RED_KAFKA_PACKAGE_VERSION` before `sdist`.
- Ensure `PKG-INFO` in generated `tar.gz` shows the same RC version.

### 10.2 Pip downloads `tar.gz` instead of wheel

Root cause:

- No wheel matches target Python ABI/platform.

Fix:

- Build matching wheel for the target runtime (for example, `cp312` for Python 3.12).
- Upload both `sdist` and matching `wheel`.
