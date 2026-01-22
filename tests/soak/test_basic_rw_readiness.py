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
    topic = os.environ.get("SOAK_TOPIC", "mcft_topic_p10")
    rate = os.environ.get("SOAK_RATE", "20")
    max_no_progress_seconds = os.environ.get("SOAK_MAX_NO_PROGRESS_SECONDS", "300")
    check_interval_seconds = os.environ.get("SOAK_CHECK_INTERVAL_SECONDS", "10")
    client_mode = os.environ.get("SOAK_CLIENT_MODE", "r")
    message_prefix = os.environ.get("SOAK_MESSAGE_PREFIX", "red-soak")
    message_marker = os.environ.get("SOAK_MESSAGE_MARKER", "")
    group = os.environ.get("SOAK_GROUP", "")
    offset_reset = os.environ.get("SOAK_OFFSET_RESET", "latest")
    default_metrics_env_config = "prod" if client_mode == "r" else ""
    metrics_env_config = os.environ.get("SOAK_METRICS_ENV_CONFIG", default_metrics_env_config)
    metrics_collect_url = os.environ.get("SOAK_METRICS_COLLECT_URL", "")

    cmd = [
        sys.executable,
        "tests/soak/soakclient.py",
        "-b", brokers,
        "-t", topic,
        "-r", rate,
        "--duration-seconds", str(duration_seconds),
        "--max-no-progress-seconds", str(max_no_progress_seconds),
        "--health-check-interval-seconds", str(check_interval_seconds),
        "--client-mode", str(client_mode),
        "--message-prefix", str(message_prefix),
        "--offset-reset", str(offset_reset),
    ]

    if message_marker:
        cmd.extend(["--message-marker", str(message_marker)])

    if group:
        cmd.extend(["--group", str(group)])

    if client_mode == "r" and metrics_env_config:
        cmd.extend(["--metrics-env-config", str(metrics_env_config)])

    if client_mode == "r" and metrics_collect_url:
        cmd.extend(["--metrics-collect-url", str(metrics_collect_url)])

    proc = subprocess.run(cmd)
    assert proc.returncode == 0
