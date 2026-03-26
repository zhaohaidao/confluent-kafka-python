# Red Kafka Python SDK 接入文档

## 1. Python 版本要求

- 最低要求：Python `3.11`

## 2. 安装方式

```bash
pip install --no-cache-dir \
  -i http://pypi.devops.xiaohongshu.com/simple/ \
  --trusted-host pypi.devops.xiaohongshu.com \
  red-kafka==0.1rc6
```

如果使用 EDS 地址，还需要准备环境变量：

```bash
export XHS_ENV=staging
export XHS_SERVICE=kafka-service-devtest
export XHS_REGION=qc-sh
export XHS_ZONE=qcsh5
export EDS_HTTP_HOST=10.11.177.52:8085
```

## 3. 基本读写 Example

```python
from confluent_kafka import Producer, Consumer


bootstrap = "eds://kafka-eds-paastest"
topic = "mcft_topic_p10"


producer = Producer({"bootstrap.servers": bootstrap})
producer.produce(topic=topic, key="demo-key", value="hello from red-kafka")
producer.flush(10)


consumer = Consumer(
    {
        "bootstrap.servers": bootstrap,
        "group.id": "replace-with-unique-group-id",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([topic])

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
