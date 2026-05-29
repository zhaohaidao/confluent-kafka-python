# Soak Readiness Test

This directory contains a long-running basic read/write readiness test based on `soakclient.py`.

By default `soakclient.py` uses `RProducer` and `RConsumer` (`--client-mode r`).
You can switch to classic clients with `--client-mode classic`.
The long-running flow follows the `examples/red_producer.py`, `examples/red_consumer.py`,
and `examples/red_roundtrip.py` pattern: write with `RProducer`, read with `RConsumer`,
and scope consumption by a run-level marker.

## 3-day readiness run

Set up a Kafka cluster first, then run:

```bash
RUN_SOAK_READINESS=1 \
SOAK_BOOTSTRAP_SERVERS=broker1.example.com:9092,broker2.example.com:9092 \
SOAK_TOPIC=red_kafka_soak \
SOAK_DURATION_SECONDS=259200 \
SOAK_RATE=20 \
SOAK_MAX_NO_PROGRESS_SECONDS=300 \
SOAK_CHECK_INTERVAL_SECONDS=10 \
SOAK_METRICS_ENV_CONFIG=prod \
pytest -q -rs tests/soak/test_basic_rw_readiness.py
```

## Direct run without pytest

```bash
python tests/soak/soakclient.py \
  -b broker1.example.com:9092,broker2.example.com:9092 \
  -t red_kafka_soak \
  -r 20 \
  --client-mode r \
  --message-prefix red-soak \
  --offset-reset latest \
  --metrics-env-config prod \
  --duration-seconds 259200 \
  --max-no-progress-seconds 300 \
  --health-check-interval-seconds 10 \
  --diagnostic-interval-seconds 300 \
  --diagnostic-file .ignore/soak-runs/readiness-diagnostics.jsonl
```

## Pass/Fail criteria

- Pass: process exits with code `0` after reaching the configured duration.
- Fail: process exits with code `2` if there is no delivery/consume progress within `max-no-progress-seconds`.
- Note: strict partition offset gap checks are disabled by default for shared topics; enable with `--check-offset-gaps` only on dedicated topics.

## Background run helper

You can submit a background soak task with:

```bash
SOAK_BOOTSTRAP_SERVERS=broker1.example.com:9092,broker2.example.com:9092 \
SOAK_TOPIC=red_kafka_soak \
SOAK_CLIENT_MODE=r \
SOAK_MESSAGE_PREFIX=red-soak \
SOAK_DURATION_SECONDS=259200 \
SOAK_MAX_NO_PROGRESS_SECONDS=300 \
SOAK_DIAGNOSTIC_INTERVAL_SECONDS=300 \
SOAK_METRICS_ENV_CONFIG=prod \
./.ignore/start-soak-readiness-bg.sh
```

The helper writes per-run artifacts to `.ignore/soak-runs/<run-id>/`.

## Diagnostics content

Each diagnostic line contains:

- runtime and progress counters (`produced`, `delivered`, `consumed`)
- message-level lag (`end_to_end_lag_messages`)
- `last_progress_age_seconds`
- partition high-watermarks seen by consumer
- message scope info (`message_prefix`, `message_marker`, `foreign_messages_skipped`)
- error histograms (`delivery_errors`, `consumer_poll_errors`, `consumer_error_callbacks`,
  `producer_error_callbacks`, `commit_errors`, `deserialize_errors`)
