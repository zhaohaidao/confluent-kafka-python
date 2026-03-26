# Red Kafka 私有 PyPI 接入报告（2026-03-26）

## 1. 概要

- 包名：`red-kafka`
- 推荐版本：`0.1rc3`
- 包类型：self-contained wheel
- 目标平台：`cp311-cp311-manylinux_2_28_x86_64`
- 私有 PyPI 源：`http://pypi.devops.xiaohongshu.com/simple/`

这版 wheel 已经内置了 `librdkafka` 及其运行时依赖。
按当前仓库内留存的构建与安装记录整理，目标环境安装后无需再手动设置 `LD_LIBRARY_PATH`。


## 2. 包获取方式

### 2.1 通过私有 PyPI 安装

```bash
pip install --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==0.1rc3
```

### 2.2 只下载 wheel

```bash
pip download --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==0.1rc3
```

### 2.3 当前工作区内的本地制品路径

当前工作区中已验证通过的 self-contained wheel 路径如下：

`/home/admin/mh/kafka/confluent-kafka-python/.ignore/private_pypi_dist_2026-03-26-rc3-selfcontained/red_kafka-0.1rc3-cp311-cp311-manylinux_2_28_x86_64.whl`


## 3. 验证结果

基于当前仓库内留存的构建、上传与安装记录，本版报告采用如下结果：

1. 从私有 PyPI 安装：`red-kafka==0.1rc3`
2. 在不设置 `LD_LIBRARY_PATH` 的情况下直接 `import confluent_kafka`
3. 成功构造 `Producer`、`Consumer` 和 `AdminClient`
4. 运行时包版本正确显示为 `0.1rc3`

对应的运行时信息如下：

- `confluent_kafka.version()` -> `('0.1rc3', 65536)`
- `confluent_kafka.libversion()` -> `('1.1.2-20-g96dc07-dirty', 16777727)`


## 4. Producer 示例

### 4.1 匿名写入示例

匿名访问请使用 `9092` 监听地址。

```python
from confluent_kafka import Producer


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
        return
    print(
        f"delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
    )


producer = Producer(
    {
        "bootstrap.servers": (
            "10.142.247.201:9092,"
            "10.142.247.204:9092,"
            "10.142.247.205:9092"
        )
    }
)

producer.produce(
    topic="your_topic",
    key="demo-key",
    value="hello from anonymous producer",
    on_delivery=delivery_report,
)
producer.flush(10)
```

### 4.2 实名鉴权写入示例

实名鉴权访问请使用 `9093` 监听地址，并带上 SASL 配置。

```python
from confluent_kafka import Producer


producer = Producer(
    {
        "bootstrap.servers": (
            "10.142.247.201:9093,"
            "10.142.247.204:9093,"
            "10.142.247.205:9093"
        ),
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-256",
        "sasl.username": "your_username",
        "sasl.password": "your_password",
        "sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule "
            "required username=\"your_username\" "
            "password=\"your_password\";"
        ),
    }
)

producer.produce(
    topic="your_topic",
    key="demo-key",
    value="hello from authenticated producer",
)
producer.flush(10)
```


## 5. Consumer 示例

### 5.1 匿名读取示例

```python
from confluent_kafka import Consumer


consumer = Consumer(
    {
        "bootstrap.servers": (
            "10.142.247.201:9092,"
            "10.142.247.204:9092,"
            "10.142.247.205:9092"
        ),
        "group.id": "demo-anonymous-consumer-group",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(["your_topic"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consume error: {msg.error()}")
            continue
        print(
            f"received key={msg.key()} value={msg.value()} "
            f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )
        break
finally:
    consumer.close()
```

### 5.2 实名鉴权读取示例

```python
from confluent_kafka import Consumer


consumer = Consumer(
    {
        "bootstrap.servers": (
            "10.142.247.201:9093,"
            "10.142.247.204:9093,"
            "10.142.247.205:9093"
        ),
        "group.id": "demo-auth-consumer-group",
        "auto.offset.reset": "earliest",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanisms": "SCRAM-SHA-256",
        "sasl.username": "your_username",
        "sasl.password": "your_password",
        "sasl.jaas.config": (
            "org.apache.kafka.common.security.scram.ScramLoginModule "
            "required username=\"your_username\" "
            "password=\"your_password\";"
        ),
    }
)

consumer.subscribe(["your_topic"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consume error: {msg.error()}")
            continue
        print(
            f"received key={msg.key()} value={msg.value()} "
            f"topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}"
        )
        break
finally:
    consumer.close()
```


## 6. 最小 Smoke Check

安装完成后，可以运行下面这段脚本做最小验证：

```bash
python - <<'PY'
from confluent_kafka import Producer, Consumer, version, libversion
from confluent_kafka.admin import AdminClient

print("pkg_version=", version())
print("lib_version=", libversion())

producer = Producer({"bootstrap.servers": "127.0.0.1:1"})
consumer = Consumer({"bootstrap.servers": "127.0.0.1:1", "group.id": "smoke-check"})
admin = AdminClient({"bootstrap.servers": "127.0.0.1:1"})

print(type(producer).__name__)
print(type(consumer).__name__)
print(type(admin).__name__)

consumer.close()
PY
```


## 7. 适用范围与限制

- 当前报告覆盖的 wheel 仅适用于 Python `3.11`
- 当前报告覆盖的 wheel 仅适用于 Linux `x86_64`
- 当前 wheel 的平台标签为 `manylinux_2_28_x86_64`，目标环境需要 glibc `2.28+`
- 如果目标环境的 glibc 版本更低，则需要按对应兼容基线重新构建 wheel
