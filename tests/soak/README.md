# Soak Readiness Test

This directory contains a long-running basic read/write readiness test based on `soakclient.py`.

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
  --duration-seconds 259200 \
  --max-no-progress-seconds 300 \
  --health-check-interval-seconds 10
```

## Pass/Fail criteria

- Pass: process exits with code `0` after reaching the configured duration.
- Fail: process exits with code `2` if there is no delivery/consume progress within `max-no-progress-seconds`.
