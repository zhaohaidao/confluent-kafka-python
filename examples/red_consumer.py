import argparse
import os
import sys
import time

# Allow running from the source tree without installation.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from confluent_kafka.red import RConsumer


def _parse_args():
    parser = argparse.ArgumentParser(description="Red Kafka consumer demo")
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
        "--group",
        default="red-kafka-demo-group",
        help="Consumer group id",
    )
    parser.add_argument(
        "--offset-reset",
        default="latest",
        choices=["earliest", "latest"],
        help="Auto offset reset policy",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=10,
        help="Stop after reading this many messages",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Poll timeout in seconds",
    )
    parser.add_argument(
        "--filter-prefix",
        default="",
        help="Only print/count messages with this value prefix",
    )
    return parser.parse_args()


def main():
    args = _parse_args()
    conf = {
        "bootstrap.servers": args.bootstrap,
        "group.id": args.group,
        "auto.offset.reset": args.offset_reset,
        "enable.auto.commit": True,
    }

    consumer = RConsumer(conf)
    consumer.subscribe([args.topic])

    received = 0
    prefix = args.filter_prefix.encode("utf-8") if args.filter_prefix else None
    try:
        while received < args.max_messages:
            msg = consumer.poll(timeout=args.timeout)
            if msg is None:
                continue
            if msg.error():
                sys.stderr.write(f"Consumer error: {msg.error()}\n")
                continue
            value = msg.value()
            if prefix and (value is None or not value.startswith(prefix)):
                continue
            sys.stdout.write(
                f"{msg.topic()} [{msg.partition()}] {msg.offset()}: {value}\n"
            )
            received += 1
            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
