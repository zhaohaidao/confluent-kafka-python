# Python SDK 构建与发布 SOP

本文档面向第一次构建 `red-kafka` Python SDK 的用户。默认路径是：**用已经准备好的 `librdkafka` manylinux 镜像构建 wheel**。不要优先在宿主机手工拼 `librdkafka`、OpenSSL、Python header 和动态库路径。

所有命令默认在 `confluent-kafka-python` 仓库根目录执行。

## 你要先知道什么

Python SDK 对外交付的是 PyPI 包：

```text
red-kafka==<version>
```

一次发布会产出 3 个文件：

```text
dist/red_kafka-<version>.tar.gz
wheelhouse/red_kafka-<version>-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
wheelhouse/red_kafka-<version>-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
```

如果你只是想“本地构建成功”，做到“本地 wheel 冒烟验证”即可，不需要上传 PyPI。

## 最短成功路径

### 1. 填两个变量

把 `<next-rc-version>` 换成要发布的版本，例如 `0.1rc18`。把镜像换成本次指定的 `librdkafka` 镜像。

```bash
export RED_KAFKA_PACKAGE_VERSION=<next-rc-version>
export LIBRDKAFKA_IMAGE=docker-reg.devops.xiaohongshu.com/media/red-kafka-python-librdkafka:<librdkafka-commit-short>
```

如果你只是本地试构建，也可以使用当前已验证过的镜像示例。这个版本号只用于本地验证，不要上传 PyPI：

```bash
export RED_KAFKA_PACKAGE_VERSION=0.1rc0
export LIBRDKAFKA_IMAGE=docker-reg.devops.xiaohongshu.com/media/red-kafka-python-librdkafka:c46df5a
```

发布正式 rc 时必须使用真实的 rc 版本号，例如 `0.1rc18`。

### 2. 检查 Docker 和镜像

```bash
docker version
docker pull "$LIBRDKAFKA_IMAGE"
docker run --rm "$LIBRDKAFKA_IMAGE" bash -lc '
set -e
ls -l /opt/librdkafka/include/librdkafka/rdkafka.h
ls -l /opt/librdkafka/lib/librdkafka.so
ls -l /opt/python/cp311-cp311/bin/python
ls -l /opt/python/cp312-cp312/bin/python
auditwheel --version
echo "librdkafka commit: ${LIBRDKAFKA_COMMIT:-unknown}"
'
```

如果 `docker pull` 失败，先处理 Docker registry 登录或镜像不存在的问题；不要继续构建。

### 3. 跑核心单测

默认跑全套核心测试：

```bash
IMAGE_TAG="$LIBRDKAFKA_IMAGE" \
RED_KAFKA_PACKAGE_VERSION="$RED_KAFKA_PACKAGE_VERSION" \
./ci/run-core-unit-tests-in-librdkafka-image.sh
```

只改 EDS 路由时，可以只跑单个文件：

```bash
IMAGE_TAG="$LIBRDKAFKA_IMAGE" \
RED_KAFKA_PACKAGE_VERSION="$RED_KAFKA_PACKAGE_VERSION" \
./ci/run-core-unit-tests-in-librdkafka-image.sh tests/test_red_eds.py
```

说明：`ci/run-core-unit-tests-in-librdkafka-image.sh` 读取的镜像变量叫 `IMAGE_TAG`，所以这里要把 `LIBRDKAFKA_IMAGE` 映射给它。

### 4. 构建 wheel 和 sdist

```bash
rm -rf build dist wheelhouse red_kafka.egg-info
mkdir -p dist wheelhouse

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
  # wheelhouse 在容器里是 /io/wheelhouse，即宿主机 $PWD/wheelhouse。
  auditwheel repair /tmp/red-kafka-wheel/red_kafka-*.whl -w wheelhouse
done

rm -rf build red_kafka.egg-info
/opt/python/cp311-cp311/bin/python setup.py sdist -d dist
'

find dist wheelhouse -maxdepth 1 -type f -printf "%f\n" | sort
sha256sum dist/* wheelhouse/*
```

### 5. 本地 wheel 冒烟验证

这一步确认刚构建出来的 wheel 可以在干净环境导入和创建 `Producer` / `Consumer`。

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
  local venv="/tmp/smoke-${spec}"
  rm -rf "$venv"
  "$py" -m venv "$venv"
  . "$venv/bin/activate"
  wheel="$(ls /io/wheelhouse/red_kafka-${RED_KAFKA_PACKAGE_VERSION}-${spec}-manylinux*.whl)"
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

必须看到 `python package` 输出中的版本等于 `RED_KAFKA_PACKAGE_VERSION`。如果看到 `1.3.0`，通常说明你在仓库目录直接运行了 Python，源码目录覆盖了 wheel；按上面的命令在容器 `/tmp` 下重试。

做到这里，本地构建已经成功。

## 发布到内部 PyPI

只有需要正式发布时才继续执行本节。

### 1. 确认版本没有发布过

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

如果输出 `exists: True`，不要覆盖同名版本，递增到下一个 rc 后重新构建。

### 2. 检查 PyPI 上传配置

确认 `~/.pypirc` 里有内部仓库条目：

```text
[red]
repository = http://pypi.devops.xiaohongshu.com
```

本文档不记录用户名、密码或 token。没有凭据时，先找内部 PyPI 维护者开通。

安装上传工具并做 metadata 检查：

```bash
python -m pip install twine
python -m twine check dist/* wheelhouse/*
```

当前仓库可能有 `long_description` warning；只要 `twine check` 返回 0，可以继续。

### 3. 上传

```bash
python -m twine upload -r red dist/* wheelhouse/*
```

### 4. 从远端重新安装验证

这一步确认包已经真正进入内部 PyPI，不是只验证了本地文件。

```bash
docker run --rm \
  -e "RED_KAFKA_PACKAGE_VERSION=$RED_KAFKA_PACKAGE_VERSION" \
  "$LIBRDKAFKA_IMAGE" \
  bash -lc '
set -euo pipefail
cd /tmp

verify_one() {
  local spec="$1"
  local py="/opt/python/${spec}/bin/python"
  local venv="/tmp/remote-${spec}"
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
}

verify_one cp311-cp311
verify_one cp312-cp312
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

## 如果没有现成 `librdkafka` 镜像

先用本仓库的 CI 脚本构建一个镜像。你需要有一个本地 `librdkafka` git 仓库或 worktree。`LIBRDKAFKA_REPO` 必须显式设置；脚本默认值指向开发者本机绝对路径，在其他机器上无效。

```bash
export LIBRDKAFKA_REPO=/path/to/librdkafka
export LIBRDKAFKA_REF=<commit-or-branch>

ci/build-librdkafka-c-only-image.sh

# 脚本会自动使用 red-kafka-python-librdkafka:<short-commit> 作为本地镜像 tag。
export LIBRDKAFKA_IMAGE=red-kafka-python-librdkafka:$(git -C "$LIBRDKAFKA_REPO" rev-parse --short "$LIBRDKAFKA_REF")
```

如果这个镜像要给别人或 CI 使用，打内部仓库 tag 并 push：

```bash
REMOTE_IMAGE="docker-reg.devops.xiaohongshu.com/media/${LIBRDKAFKA_IMAGE}"
docker tag "$LIBRDKAFKA_IMAGE" "$REMOTE_IMAGE"
docker push "$REMOTE_IMAGE"
export LIBRDKAFKA_IMAGE="$REMOTE_IMAGE"
```

构建脚本会把 `LIBRDKAFKA_COMMIT` 写入镜像环境变量。发布记录里必须记录这个 commit。

## 本地开发构建

只有你想在宿主机直接调试 C 扩展时才用本节。小白用户优先使用前面的容器路径。

```bash
export VENV="$PWD/.venv-dev"
python3.11 -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

如果 `librdkafka` 不在系统默认路径，设置：

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

macOS 使用 `DYLD_LIBRARY_PATH` 替代 `LD_LIBRARY_PATH`。

## 发布记录模板

发布完成后记录以下信息：

```bash
git log -1 --oneline
docker run --rm "$LIBRDKAFKA_IMAGE" bash -lc 'echo "${LIBRDKAFKA_COMMIT:?LIBRDKAFKA_COMMIT not set in image}"'
sha256sum dist/* wheelhouse/*
```

如果 `LIBRDKAFKA_COMMIT not set in image` 报错，说明该镜像构建时未注入 commit 变量。必须从镜像构建日志或 `ci/build-librdkafka-c-only-image.sh` 的构建输出中补记 `librdkafka` commit，不能留空。

模板：

```text
Python package: red-kafka==<version>
Python source commit: <commit-id> (<commit-message>)
librdkafka image: <image>
librdkafka source commit: <commit-id>
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

### `docker pull` 失败

通常是没有登录内部 registry，或者镜像 tag 写错。先处理 Docker 登录和镜像权限。

### 宿主机 import 失败

如果报 `librdkafka.so: cannot open shared object file` 或 `libssl.so.10: cannot open shared object file`，优先用 `LIBRDKAFKA_IMAGE` 运行测试和构建。不要在宿主机临时拼系统库后直接发布。

### wheel 冒烟误用了源码目录

不要在仓库根目录直接运行冒烟 Python。`/io/confluent_kafka` 会覆盖虚拟环境中的 wheel 包，可能导致 `No module named 'confluent_kafka.cimpl'` 或版本显示异常。冒烟测试必须先 `cd /tmp`。

### 版本已经存在

内部 PyPI 通常不允许覆盖同名文件。若 `RED_KAFKA_PACKAGE_VERSION` 已存在，递增到下一个 rc 版本并重新构建全部产物。

### 只改 Python 代码，为什么还要指定 `librdkafka` 镜像

wheel 中包含编译后的 C extension，构建时必须链接一个确定版本的 `librdkafka`。发布记录必须能追溯这个 `librdkafka` 版本，否则线上问题无法复现。
