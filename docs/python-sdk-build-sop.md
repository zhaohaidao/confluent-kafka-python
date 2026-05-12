# Python SDK 构建 SOP

本文档说明如何从当前仓库构建、验证和打包 `red-kafka` Python SDK。所有命令默认在仓库根目录执行。

## 适用范围

- 构建依赖 `librdkafka` 的本地 C 扩展。
- 在发布或交付构建产物前执行最小验证。
- 产出源码包和 wheel 包，同时避免依赖特定机器的绝对路径。

## 前置条件

- Python 3.11 或更高版本。
- 可用的 C 编译器和 Python 开发头文件。
- `pip`、`setuptools` 和 `wheel`。
- `librdkafka` 头文件和共享库。

如果 `librdkafka` 安装在非系统默认路径，用环境变量声明安装前缀，并从该前缀派生 include、library 和运行时库路径：

```bash
export LIBRDKAFKA_PREFIX=/path/to/librdkafka-prefix
export C_INCLUDE_PATH="$LIBRDKAFKA_PREFIX/include${C_INCLUDE_PATH:+:$C_INCLUDE_PATH}"
export LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LIBRARY_PATH:+:$LIBRARY_PATH}"
export LD_LIBRARY_PATH="$LIBRDKAFKA_PREFIX/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```

在 macOS 上，如果需要设置运行时动态库搜索路径，使用 `DYLD_LIBRARY_PATH` 替代 `LD_LIBRARY_PATH`。

## 环境准备

使用独立虚拟环境：

```bash
python3.11 -m venv "$VENV"
. "$VENV/bin/activate"
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"
```

`$VENV` 由操作者指定，可以放在当前 workspace 下，也可以放在其他本地临时目录中。不要提交虚拟环境或构建输出。

## 本地构建

构建本地扩展：

```bash
python setup.py build
```

如果 `librdkafka` 不在系统默认 include/library 路径中，保持前面提到的 `C_INCLUDE_PATH`、`LIBRARY_PATH` 和运行时动态库路径已导出。

验证构建后的扩展可以正常导入，并确认链接到的库版本：

```bash
python - <<'PY'
import confluent_kafka

print("python package:", confluent_kafka.version())
print("librdkafka:", confluent_kafka.libversion())
PY
```

## `librdkafka` 版本口径

发布构建必须显式声明 Python SDK 依赖的 `librdkafka` 动态库版本。不要把脚本默认值当作发布版本口径。

当前仓库 CI 中的默认版本来自 `.travis.yml` 的 `LIBRDKAFKA_VERSION`，当前值为：

```bash
LIBRDKAFKA_VERSION=v1.3.0
```

发布 wheel 时推荐使用固定 tag：

```bash
export LIBRDKAFKA_VERSION=v1.3.0
tools/cibuildwheel-build.sh wheelhouse "$LIBRDKAFKA_VERSION"
```

如果必须基于内部 patch 过的 `librdkafka` 源码构建，使用 `RDKAFKA_SOURCE_DIR` 指向源码目录，并在 PR 或 release notes 中记录源码仓库、commit id 和 commit message：

```bash
export RDKAFKA_SOURCE_DIR=/path/to/librdkafka-source
tools/cibuildwheel-build.sh wheelhouse
```

版本描述建议使用以下格式：

```text
Python SDK wheels bundle dynamic librdkafka built from <source>, version/tag <tag-or-version>.
For patched builds, librdkafka source commit is <commit-id> (<commit-message>).
The Python extension links dynamically to librdkafka; source installs require a compatible librdkafka to be available on the build host.
```

示例：

```text
Python SDK wheels bundle dynamic librdkafka built from upstream tag v1.3.0.
The Python extension links dynamically to librdkafka; source installs require a compatible librdkafka to be available on the build host.
```

## 验证

运行 lint 和单元测试：

```bash
python -m flake8
python -m pytest -q
```

如果已安装 `tox`，并且本机具备 `tox.ini` 需要的 Python 解释器，可以运行完整 tox 矩阵：

```bash
tox
```

针对 RED runtime 相关改动，优先运行聚焦测试：

```bash
python -m pytest -q tests/test_red_eds.py tests/test_red_metrics.py tests/test_public_clients.py
```

集成测试依赖 Docker 和 Kafka 测试配置。依赖就绪时，通过项目测试入口运行：

```bash
./tests/run.sh unit
./tests/run.sh all
```

## 打包产物

打包前显式指定发布版本：

```bash
export RED_KAFKA_PACKAGE_VERSION=<version>
rm -rf build dist wheelhouse red_kafka.egg-info
python setup.py sdist bdist_wheel
python -m pip wheel . --no-deps --wheel-dir wheelhouse
```

预期产物：

- `dist/red-kafka-<version>.tar.gz`
- `dist/red_kafka-<version>-*.whl`
- `wheelhouse/red_kafka-<version>-*.whl`

不要提交 `build/`、`dist/`、`wheelhouse/` 或 `*.egg-info/`。

## 产物冒烟验证

在全新的虚拟环境中安装 wheel 并运行导入冒烟测试：

```bash
python3.11 -m venv "$SMOKE_VENV"
. "$SMOKE_VENV/bin/activate"
python -m pip install --upgrade pip
python -m pip install dist/red_kafka-<version>-*.whl
python - <<'PY'
from confluent_kafka import Consumer, Producer, libversion, version

print("python package:", version())
print("librdkafka:", libversion())
Producer({"bootstrap.servers": "127.0.0.1:1"}).flush(0)
Consumer({
    "bootstrap.servers": "127.0.0.1:1",
    "group.id": "smoke-test",
}).close()
PY
```

如果冒烟测试出现动态库加载错误，确认运行时动态库路径指向的 `librdkafka` 前缀，与构建时使用的前缀一致。

## 发布检查清单

- 确认 `RED_KAFKA_PACKAGE_VERSION` 是预期发布版本。
- 确认发布构建使用的 `LIBRDKAFKA_VERSION` 或 `RDKAFKA_SOURCE_DIR` 已记录；如果使用 patch 过的源码，记录源码 commit id 和 commit message。
- 确认 `python -m flake8` 和相关 `pytest` 测试已通过。
- 确认 wheel 在全新虚拟环境中的冒烟测试已通过。
- 确认生成的构建产物未被 Git 跟踪，除非发布流程明确要求提交。
- 在 release notes 或 PR 描述中记录本次构建使用的 `librdkafka` 版本。
