# Python SDK 构建 SOP

## 0. 启动容器

启动已经包含 `/opt/librdkafka` 的构建镜像，并把 bash 当前目录设置为代码库根目录：

```bash
cd "$(git rev-parse --show-toplevel)"
export LIBRDKAFKA_IMAGE=docker-reg.devops.xiaohongshu.com/media/red-kafka-python-librdkafka:280be6f

docker run --rm -it \
  -v "$PWD:$PWD" \
  -w "$PWD" \
  "$LIBRDKAFKA_IMAGE" \
  bash
```

办公网络不需要设置代理；如果容器内访问内网 PyPI 失败，再设置代理：

```bash
export http_proxy=http://10.3.4.34:3128
export https_proxy=http://10.3.4.34:3128
export HTTP_PROXY=http://10.3.4.34:3128
export HTTPS_PROXY=http://10.3.4.34:3128
env | grep -i proxy
```

## 1. 构建并验证

执行目录：代码库根目录。

```bash
./ci/build-python-package-in-librdkafka-image.sh
```

## 2. 产物

脚本成功后会输出产物文件名和 sha256：

```text
red_kafka-<version>.tar.gz
red_kafka-<version>-cp311-cp311-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
red_kafka-<version>-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
```

不要提交 `build/`、`dist/`、`wheelhouse/` 或 `red_kafka.egg-info/`。

## 3. 可选：发布到内网 pip 仓库

执行目录：代码库根目录。

默认只构建本地包，不发布；只有需要发布到内网 pip 仓库时，才执行本步骤。
CI 在任务环境变量里配置 `TWINE_USERNAME` / `TWINE_PASSWORD`，不要把凭据提交进代码库。

```bash
export TWINE_USERNAME=red-pypi
export TWINE_PASSWORD=xhsdev
./ci/publish-python-package-to-internal-pypi.sh 0.1rc24
```
