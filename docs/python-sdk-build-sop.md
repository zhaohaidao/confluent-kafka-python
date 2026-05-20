# Python SDK 本地构建 SOP

目标只有一个：在本地构建出 `red-kafka` Python 包文件。本文档不包含测试、不包含上传 PyPI、不包含发布验证。

所有命令在仓库根目录执行。

## 前置条件

需要 Docker 能拉取一个已经包含 `librdkafka` 的 manylinux 镜像。镜像里至少要有：

- `/opt/librdkafka/include/librdkafka/rdkafka.h`
- `/opt/librdkafka/lib/librdkafka.so`
- `/opt/python/cp311-cp311/bin/python`
- `/opt/python/cp312-cp312/bin/python`
- `auditwheel`

## 1. 设置版本和镜像

```bash
export RED_KAFKA_PACKAGE_VERSION=<version>
export LIBRDKAFKA_IMAGE=<librdkafka-manylinux-image>
```

示例：

```bash
export RED_KAFKA_PACKAGE_VERSION=0.1rc18
export LIBRDKAFKA_IMAGE=docker-reg.devops.xiaohongshu.com/media/red-kafka-python-librdkafka:c46df5a
```

## 2. 确认镜像可用

```bash
docker pull "$LIBRDKAFKA_IMAGE"
docker run --rm "$LIBRDKAFKA_IMAGE" bash -lc '
set -e
ls -l /opt/librdkafka/include/librdkafka/rdkafka.h
ls -l /opt/librdkafka/lib/librdkafka.so
ls -l /opt/python/cp311-cp311/bin/python
ls -l /opt/python/cp312-cp312/bin/python
auditwheel --version
'
```

如果这里失败，先处理 Docker 登录、镜像 tag 或镜像内容问题。

## 3. 构建包

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
  auditwheel repair /tmp/red-kafka-wheel/red_kafka-*.whl -w wheelhouse
done

rm -rf build red_kafka.egg-info
/opt/python/cp311-cp311/bin/python setup.py sdist -d dist
'
```

## 4. 查看产物

```bash
find dist wheelhouse -maxdepth 1 -type f -printf "%f\n" | sort
sha256sum dist/* wheelhouse/*
```

正常会看到 3 个文件：

```text
red_kafka-<version>.tar.gz
red_kafka-<version>-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
red_kafka-<version>-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
```

不要提交 `build/`、`dist/`、`wheelhouse/` 或 `red_kafka.egg-info/`。
