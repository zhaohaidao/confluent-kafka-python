# Python SDK 构建与发布 SOP

本文档说明如何构建、验证并发布 `red-kafka` Python SDK。所有命令默认在仓库根目录执行。

## 适用范围

- 本地开发时构建依赖 `librdkafka` 的 C 扩展。
- 在 CI 或发布环境中构建 manylinux wheel 和 sdist。
- 发布到内部 PyPI，并从远端重新安装验证。

本文档不覆盖真实 Kafka 集群集成测试。集成测试需要单独准备 broker 集群和 `testconf.json`。

## 产物口径

Python SDK 对外交付的是 PyPI 包：

```text
red-kafka==<version>
```

发布时必须同时记录：

- Python SDK 源码 commit。
- `RED_KAFKA_PACKAGE_VERSION`。
- 构建 wheel 使用的 `librdkafka` 镜像 tag 或源码 commit。
- 上传后的内部 PyPI index。

内部 PyPI 安装命令：

```bash
python -m pip install --pre \
  -i http://pypi.devops.xiaohongshu.com/simple \
  --trusted-host pypi.devops.xiaohongshu.com \
  "red-kafka==<version>"
```

## 发布前检查

先确认工作区和目标版本：

```bash
git status --short --branch
git log -1 --oneline

export RED_KAFKA_PACKAGE_VERSION=<version>
export LIBRDKAFKA_IMAGE=<image-containing-librdkafka>
```

示例：

```bash
export RED_KAFKA_PACKAGE_VERSION=0.1rc17
export LIBRDKAFKA_IMAGE=docker-reg.devops.xiaohongshu.com/media/red-kafka-python-librdkafka:c46df5a
```

`LIBRDKAFKA_IMAGE` 必须包含：

- `/opt/librdkafka/include/librdkafka/rdkafka.h`
- `/opt/librdkafka/lib/librdkafka.so`
- `/opt/python/cp311-cp311/bin/python`
- `/opt/python/cp312-cp312/bin/python`
- `auditwheel`

检查镜像：

```bash
docker run --rm "$LIBRDKAFKA_IMAGE" bash -lc '
set -e
ls -l /opt/librdkafka/include/librdkafka/rdkafka.h
ls -l /opt/librdkafka/lib/librdkafka.so
ls -l /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python
auditwheel --version
'
```

确认目标版本尚未发布：

```bash
python - <<'PY'
import os
import re
import urllib.request

version = os.environ["RED_KAFKA_PACKAGE_VERSION"]
with urllib.request.urlopen(
    "http://pypi.devops.xiaohongshu.com/simple/red-kafka/",
    timeout=10,
) as response:
    html = response.read().decode()
exists = bool(re.search(r"red_kafka-%s[.-]" % re.escape(version), html))
print("version:", version)
print("exists:", exists)
raise SystemExit(1 if exists else 0)
PY
```

## 单元测试

优先在同一个 `librdkafka` 镜像中运行核心单测，避免宿主机缺 `libssl.so.10` 或 `librdkafka.so`：

```bash
# ci/run-core-unit-tests-in-librdkafka-image.sh 读取 IMAGE_TAG；这里显式映射到发布用的 LIBRDKAFKA_IMAGE。
IMAGE_TAG="$LIBRDKAFKA_IMAGE" \
RED_KAFKA_PACKAGE_VERSION="$RED_KAFKA_PACKAGE_VERSION" \
./ci/run-core-unit-tests-in-librdkafka-image.sh \
  tests/test_red_eds.py \
  tests/test_red_metrics.py \
  tests/test_public_clients.py \
  tests/test_auth_csv_runner_tool.py \
  tests/test_Producer.py \
  tests/test_Consumer.py \
  tests/test_Admin.py \
  tests/test_misc.py
```

只改 EDS 路由时，至少运行：

```bash
# ci/run-core-unit-tests-in-librdkafka-image.sh 读取 IMAGE_TAG；这里显式映射到发布用的 LIBRDKAFKA_IMAGE。
IMAGE_TAG="$LIBRDKAFKA_IMAGE" \
RED_KAFKA_PACKAGE_VERSION="$RED_KAFKA_PACKAGE_VERSION" \
./ci/run-core-unit-tests-in-librdkafka-image.sh tests/test_red_eds.py
```

如果本机已经有匹配的动态库，也可以直接运行：

```bash
export LD_LIBRARY_PATH=<librdkafka-prefix>/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
python -m pytest -q tests/test_red_eds.py
```

宿主机缺动态库时，不要把 import 失败误判为代码回归；切回容器验证。

## 构建发布产物

清理旧产物：

```bash
rm -rf build dist wheelhouse red_kafka.egg-info
mkdir -p dist wheelhouse
```

在 `LIBRDKAFKA_IMAGE` 中构建 cp311/cp312 wheel，并用 `auditwheel` 修复成 manylinux wheel：

```bash
docker run --rm \
  -e "RED_KAFKA_PACKAGE_VERSION=$RED_KAFKA_PACKAGE_VERSION" \
  -v "$PWD:/io" \
  -w /io \
  "$LIBRDKAFKA_IMAGE" \
  bash -lc '
set -euo pipefail
export CFLAGS="-I/opt/librdkafka/include"
export LDFLAGS="-L/opt/librdkafka/lib"
export LD_LIBRARY_PATH="/opt/librdkafka/lib:${LD_LIBRARY_PATH:-}"

for py in /opt/python/cp311-cp311/bin/python /opt/python/cp312-cp312/bin/python; do
  rm -rf build red_kafka.egg-info /tmp/red-kafka-wheel
  mkdir -p /tmp/red-kafka-wheel
  "$py" -m pip wheel . --no-deps -w /tmp/red-kafka-wheel
  # wheelhouse resolves to /io/wheelhouse under -w /io, i.e. $PWD/wheelhouse on the host.
  auditwheel repair /tmp/red-kafka-wheel/red_kafka-*.whl -w wheelhouse
done

rm -rf build red_kafka.egg-info
/opt/python/cp311-cp311/bin/python setup.py sdist -d dist
'
```

预期产物：

```text
dist/red_kafka-<version>.tar.gz
wheelhouse/red_kafka-<version>-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
wheelhouse/red_kafka-<version>-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
```

计算 hash：

```bash
sha256sum dist/* wheelhouse/*
```

不要提交 `build/`、`dist/`、`wheelhouse/` 或 `*.egg-info/`。

## 本地产物验证

先检查包元数据：

```bash
python -m pip install twine
python -m twine check dist/* wheelhouse/*
```

`long_description` warning 是当前仓库既有 metadata 问题；只要 `twine check` 返回 0，可以继续发布。

再从 wheel 文件做 cp311/cp312 干净安装冒烟。注意不要在仓库目录执行 Python，否则当前源码目录会盖过已安装 wheel：

```bash
docker run --rm \
  -e "RED_KAFKA_PACKAGE_VERSION=$RED_KAFKA_PACKAGE_VERSION" \
  -v "$PWD:/io" \
  "$LIBRDKAFKA_IMAGE" \
  bash -lc '
set -euo pipefail
cd /tmp

smoke_one() {
  local spec="$1"
  local py="/opt/python/${spec}/bin/python"
  venv=/tmp/smoke-$spec
  rm -rf "$venv"
  "$py" -m venv "$venv"
  . "$venv/bin/activate"
  wheel="$(ls /io/wheelhouse/red_kafka-${RED_KAFKA_PACKAGE_VERSION}-${spec}-manylinux*.whl)"
  # "$wheel" is a local file; the index is only used to resolve Python dependencies such as requests.
  python -m pip install \
    -i http://pypi.devops.xiaohongshu.com/simple \
    --trusted-host pypi.devops.xiaohongshu.com \
    "$wheel"
  python - <<'"'"'PY'"'"'
from confluent_kafka import Consumer, Producer, libversion, version

print("python package:", version())
print("librdkafka:", libversion())
Producer({"bootstrap.servers": "127.0.0.1:1"}).flush(0)
Consumer({
    "bootstrap.servers": "127.0.0.1:1",
    "group.id": "smoke-test",
}).close()
PY
  deactivate
}

smoke_one cp311-cp311
smoke_one cp312-cp312
'
```

期望 `version()` 输出包含 `RED_KAFKA_PACKAGE_VERSION`，`libversion()` 输出构建镜像中的 `librdkafka` 版本。

## 上传到内部 PyPI

上传前确认 `~/.pypirc` 中存在内部仓库条目，例如：

```text
[red]
repository = http://pypi.devops.xiaohongshu.com
```

不要把用户名、密码或 token 写入本文档。

上传：

```bash
python -m pip install twine
python -m twine upload -r red dist/* wheelhouse/*
```

## 远端安装验证

上传成功后，从内部 PyPI 重新安装，确认不是只验证了本地文件：

```bash
docker run --rm \
  -e "RED_KAFKA_PACKAGE_VERSION=$RED_KAFKA_PACKAGE_VERSION" \
  "$LIBRDKAFKA_IMAGE" \
  bash -lc '
set -euo pipefail
cd /tmp
for spec in cp311-cp311 cp312-cp312; do
  py=/opt/python/$spec/bin/python
  venv=/tmp/remote-$spec
  rm -rf "$venv"
  "$py" -m venv "$venv"
  . "$venv/bin/activate"
  python -m pip install --pre \
    -i http://pypi.devops.xiaohongshu.com/simple \
    --trusted-host pypi.devops.xiaohongshu.com \
    "red-kafka==${RED_KAFKA_PACKAGE_VERSION}"
  python - <<'"'"'PY'"'"'
from confluent_kafka import libversion, version

print("python package:", version())
print("librdkafka:", libversion())
PY
  deactivate
done
'
```

再检查 simple index 中的文件名和 hash：

```bash
python - <<'PY'
import os
import re
import urllib.request

version = os.environ["RED_KAFKA_PACKAGE_VERSION"]
with urllib.request.urlopen(
    "http://pypi.devops.xiaohongshu.com/simple/red-kafka/",
    timeout=10,
) as response:
    html = response.read().decode()
for name in sorted(set(re.findall(r"red_kafka-%s[^\"<>]*" % re.escape(version), html))):
    print(name)
PY
```

## 本地开发构建

本地只做开发验证时，可以不走 manylinux 镜像。先准备虚拟环境：

```bash
export VENV="$PWD/.venv-dev"
python3.11 -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

如果 `librdkafka` 安装在非系统默认路径，用安装前缀派生 include、library 和运行时库路径：

```bash
export LIBRDKAFKA_PREFIX=/path/to/librdkafka-prefix
export C_INCLUDE_PATH="$LIBRDKAFKA_PREFIX/include${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
export LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

构建并检查导入：

```bash
python setup.py build
python - <<'PY'
import confluent_kafka

print("python package:", confluent_kafka.version())
print("librdkafka:", confluent_kafka.libversion())
PY
```

在 macOS 上，如果需要设置运行时动态库搜索路径，使用 `DYLD_LIBRARY_PATH` 替代 `LD_LIBRARY_PATH`。

## 发布记录模板

发布完成后记录以下信息：

从构建镜像读取 `librdkafka` commit：

```bash
docker run --rm "$LIBRDKAFKA_IMAGE" bash -lc 'echo "${LIBRDKAFKA_COMMIT:?LIBRDKAFKA_COMMIT not set in image}"'
```

如果该命令失败，说明镜像没有记录 `librdkafka` commit；必须从镜像构建日志或镜像发布记录补证，不能留空。

```text
Python package: red-kafka==<version>
Python source commit: <commit-id> (<commit-message>)
librdkafka image: <image>
librdkafka source commit: <commit-id> (<commit-message>)
Artifacts:
- red_kafka-<version>-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl sha256=<sha256>
- red_kafka-<version>-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl sha256=<sha256>
- red_kafka-<version>.tar.gz sha256=<sha256>
Validation:
- core unit tests: <command and result>
- twine check: <result>
- local wheel smoke: cp311/cp312 <result>
- remote PyPI install: cp311/cp312 <result>
```

## 常见问题

### 宿主机 import 失败

如果报 `librdkafka.so: cannot open shared object file` 或 `libssl.so.10: cannot open shared object file`，优先用 `LIBRDKAFKA_IMAGE` 运行测试。不要在宿主机临时拼系统库后直接发布。

### wheel 冒烟误用了源码目录

在仓库根目录执行 Python 时，`/io/confluent_kafka` 可能覆盖虚拟环境中的 wheel 包，导致 `No module named 'confluent_kafka.cimpl'`。冒烟测试必须先 `cd /tmp`。

### 发布版本已存在

内部 PyPI 通常不允许覆盖同名文件。若 `RED_KAFKA_PACKAGE_VERSION` 已存在，递增到下一个 rc 版本并重新构建全部产物。
