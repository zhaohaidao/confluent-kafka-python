# Soak Readiness Test

This directory contains a long-running basic read/write readiness test based on `soakclient.py`.

By default `soakclient.py` uses `RProducer` and `RConsumer` (`--client-mode r`).
You can switch to classic clients with `--client-mode classic`.

## 3-day readiness run

Set up a Kafka cluster first, then run:

```bash
RUN_SOAK_READINESS=1 \
SOAK_BOOTSTRAP_SERVERS=localhost:9092 \
SOAK_TOPIC=soak-basic-rw-readiness \
SOAK_DURATION_SECONDS=259200 \
SOAK_RATE=20 \
SOAK_MAX_NO_PROGRESS_SECONDS=300 \
SOAK_CHECK_INTERVAL_SECONDS=10 \
pytest -q -rs tests/soak/test_basic_rw_readiness.py
```

## Direct run without pytest

```bash
python tests/soak/soakclient.py \
  -b localhost:9092 \
  -t soak-basic-rw-readiness \
  -r 20 \
  --client-mode r \
  --duration-seconds 259200 \
  --max-no-progress-seconds 300 \
  --health-check-interval-seconds 10 \
  --diagnostic-interval-seconds 300 \
  --diagnostic-file .ignore/soak-runs/readiness-diagnostics.jsonl
```

## Pass/Fail criteria

- Pass: process exits with code `0` after reaching the configured duration.
- Fail: process exits with code `2` if there is no delivery/consume progress within `max-no-progress-seconds`.

## Background run helper

You can submit a background soak task with:

```bash
SOAK_BOOTSTRAP_SERVERS=localhost:9092 \
SOAK_TOPIC=soak-basic-rw-readiness \
SOAK_CLIENT_MODE=r \
SOAK_DURATION_SECONDS=259200 \
SOAK_MAX_NO_PROGRESS_SECONDS=300 \
SOAK_DIAGNOSTIC_INTERVAL_SECONDS=300 \
./.ignore/start-soak-readiness-bg.sh
```

The helper writes per-run artifacts to `.ignore/soak-runs/<run-id>/`.

## Diagnostics content

Each diagnostic line contains:

- runtime and progress counters (`produced`, `delivered`, `consumed`)
- message-level lag (`end_to_end_lag_messages`)
- `last_progress_age_seconds`
- partition high-watermarks seen by consumer
- error histograms (`delivery_errors`, `consumer_poll_errors`, `consumer_error_callbacks`,
  `producer_error_callbacks`, `commit_errors`, `deserialize_errors`)
