#!/usr/bin/env python

import os
import subprocess
import sys

import pytest


@pytest.mark.skipif(
    os.environ.get("RUN_SOAK_READINESS") != "1",
    reason="set RUN_SOAK_READINESS=1 to run the long-running soak readiness test",
)
def test_basic_rw_readiness():
    brokers = os.environ.get("SOAK_BOOTSTRAP_SERVERS")
    if not brokers:
        pytest.skip("SOAK_BOOTSTRAP_SERVERS is required when RUN_SOAK_READINESS=1")

    duration_seconds = int(os.environ.get("SOAK_DURATION_SECONDS", "259200"))
    topic = os.environ.get("SOAK_TOPIC", "soak-basic-rw-readiness")
    rate = os.environ.get("SOAK_RATE", "20")
    max_no_progress_seconds = os.environ.get("SOAK_MAX_NO_PROGRESS_SECONDS", "300")
    check_interval_seconds = os.environ.get("SOAK_CHECK_INTERVAL_SECONDS", "10")

    cmd = [
        sys.executable,
        "tests/soak/soakclient.py",
        "-b", brokers,
        "-t", topic,
        "-r", rate,
        "--duration-seconds", str(duration_seconds),
        "--max-no-progress-seconds", str(max_no_progress_seconds),
        "--health-check-interval-seconds", str(check_interval_seconds),
    ]

    proc = subprocess.run(cmd)
    assert proc.returncode == 0
