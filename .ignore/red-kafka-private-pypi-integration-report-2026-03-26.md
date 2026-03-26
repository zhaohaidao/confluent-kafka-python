# Red Kafka Private PyPI Integration Report (2026-03-26)

## 1. Summary

- Package name: `red-kafka`
- Recommended version: `0.1rc3`
- Package type: self-contained wheel
- Verified platform: `cp311-cp311-manylinux_2_28_x86_64`
- Private PyPI index: `http://pypi.devops.xiaohongshu.com/simple/`

This wheel already bundles `librdkafka` and its required runtime libraries.
For the verified target environment, users do not need to set `LD_LIBRARY_PATH`
manually after installation.


## 2. Package Delivery

### 2.1 Private PyPI Install

```bash
pip install --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==0.1rc3
```

### 2.2 Download Wheel Only

```bash
pip download --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==0.1rc3
```

### 2.3 Local Artifact Path

The verified self-contained wheel built in this workspace is:

`/home/admin/mh/kafka/confluent-kafka-python/.ignore/private_pypi_dist_2026-03-26-rc3-selfcontained/red_kafka-0.1rc3-cp311-cp311-manylinux_2_28_x86_64.whl`


## 3. Verification Result

The following path has been verified successfully:

1. Install from private PyPI: `red-kafka==0.1rc3`
2. Import `confluent_kafka` without `LD_LIBRARY_PATH`
3. Construct `Producer`, `Consumer`, and `AdminClient`
4. Runtime package version reports `0.1rc3`

Verified runtime characteristics:

- `confluent_kafka.version()` -> `('0.1rc3', 65536)`
- `confluent_kafka.libversion()` -> `('1.1.2-20-g96dc07-dirty', 16777727)`


## 4. Producer Example

### 4.1 Anonymous Write Example

Use anonymous access against the `9092` listener.

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

### 4.2 Authenticated Write Example

Use SASL against the `9093` listener.

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


## 5. Consumer Example

### 5.1 Anonymous Read Example

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

### 5.2 Authenticated Read Example

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


## 6. Minimal Smoke Check

After installation, users can run:

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


## 7. Constraints

- This verified wheel is for Python `3.11` only.
- This verified wheel is for Linux `x86_64` only.
- The wheel tag is `manylinux_2_28_x86_64`, so the target runtime needs glibc `2.28+`.
- If lower-glibc environments must be supported, another compatibility baseline is required and the wheel must be rebuilt accordingly.
