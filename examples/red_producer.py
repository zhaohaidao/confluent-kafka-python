import argparse
import os
import sys

# Allow running from the source tree without installation.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from confluent_kafka.red import RProducer


def _delivery_report(err, msg):
    if err is not None:
        sys.stderr.write(f"Delivery failed: {err}\n")
        return
    sys.stdout.write(
        f"Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}\n"
    )


def _parse_args():
    parser = argparse.ArgumentParser(description="Red Kafka producer demo")
    parser.add_argument(
        "--bootstrap",
        default="10.13.10.79:9092,10.32.12.69:9092,10.13.2.94:9092",
        help="Bootstrap servers",
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
        help="Number of messages to produce",
    )
    parser.add_argument(
        "--prefix",
        default="red-kafka-demo",
        help="Message value prefix",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    conf = {
        "bootstrap.servers": args.bootstrap,
        "client.id": "red-producer-demo",
    }

    producer = RProducer(conf)
    for i in range(args.count):
        value = f"{args.prefix}-{i}"
        producer.produce(args.topic, value=value, on_delivery=_delivery_report)
        producer.poll(0)

    producer.flush(10)
    producer.close()


if __name__ == "__main__":
    main()
