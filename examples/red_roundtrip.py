import argparse
import os
import sys
import time
import uuid

# Allow running from the source tree without installation.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from confluent_kafka.red import RConsumer, RProducer


def _delivery_report(err, msg, state):
    if err is not None:
        state["errors"].append(str(err))
        return
    state["delivered"] += 1


def _parse_args():
    parser = argparse.ArgumentParser(description="Red Kafka roundtrip demo")
    parser.add_argument(
        "--bootstrap",
        default="10.13.10.79:9092,10.32.12.69:9092,10.13.2.94:9092",
        help="Bootstrap servers (supports eds://<service-name>)",
    )
    parser.add_argument(
        "--topic",
        default="mcft_topic_p10",
        help="Target topic",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of messages to produce and consume",
    )
    parser.add_argument(
        "--prefix",
        default="",
        help="Message value prefix (auto-generated if empty)",
    )
    parser.add_argument(
        "--group",
        default="",
        help="Consumer group id (auto-generated if empty)",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=1.0,
        help="Poll timeout in seconds",
    )
    parser.add_argument(
        "--deadline",
        type=float,
        default=30.0,
        help="Total time budget for consuming in seconds",
    )
    return parser.parse_args()


def _build_prefix(prefix):
    if prefix:
        return prefix
    return f"red-kafka-rt-{uuid.uuid4().hex}"


def _build_group(group):
    if group:
        return group
    return f"red-kafka-rt-group-{int(time.time())}"


def main():
    args = _parse_args()
    prefix = _build_prefix(args.prefix)
    group = _build_group(args.group)

    producer_conf = {
        "bootstrap.servers": args.bootstrap,
        "client.id": "red-roundtrip-producer",
    }
    consumer_conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }

    state = {"delivered": 0, "errors": []}
    producer = RProducer(producer_conf)
    for i in range(args.count):
        value = f"{prefix}-{i}"
        producer.produce(
            args.topic, value=value, on_delivery=lambda e, m: _delivery_report(e, m, state)
        )
        producer.poll(0)

    producer.flush(10)
    producer.close()

    if state["errors"]:
        sys.stderr.write("Delivery errors:\n")
        for err in state["errors"]:
            sys.stderr.write(f"{err}\n")
        sys.exit(1)

    consumer = RConsumer(consumer_conf)
    consumer.subscribe([args.topic])

    received = 0
    deadline = time.time() + args.deadline
    prefix_bytes = prefix.encode("utf-8")
    try:
        while received < args.count and time.time() < deadline:
            msg = consumer.poll(timeout=args.poll_timeout)
            if msg is None:
                continue
            if msg.error():
                sys.stderr.write(f"Consumer error: {msg.error()}\n")
                continue
            value = msg.value()
            if value is None or not value.startswith(prefix_bytes):
                continue
            sys.stdout.write(
                f"{msg.topic()} [{msg.partition()}] {msg.offset()}: {value}\n"
            )
            received += 1
    finally:
        consumer.close()

    if received < args.count:
        sys.stderr.write(
            f"Roundtrip incomplete: got {received}/{args.count} messages\n"
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
