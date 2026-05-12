import json
import logging

import pytest

from confluent_kafka import Consumer, Producer, red_metrics


class DummyClient:
    def __init__(self, stats):
        self._stats = stats

    def stats_collect(self):
        return json.dumps(self._stats)


class DummyUrlopenResponse:
    status = 204

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return b""


def test_metrics_collector_basic(monkeypatch):
    stats = {
        "name": "client",
        "time": 1700000000,
        "topics": {
            "topic_a": {"partitions": {"0": {"foo": "bar"}}},
            "topic_b": {},
        },
        "cgrp": {"state": "stable"},
    }

    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))
    red_metrics.EnvUtil._host_ip = "127.0.0.1"
    red_metrics.EnvUtil._host_name = "host"
    red_metrics.EnvUtil._appid = "app"
    red_metrics.EnvUtil._service = "svc"
    red_metrics.EnvUtil._region = "r1"
    red_metrics.EnvUtil._zone = "z1"
    red_metrics.EnvUtil._xhs_env = "test"
    red_metrics.EnvUtil._job_env = "job"
    red_metrics.EnvUtil._config_env = "cfg"

    collector = red_metrics.MetricsCollector(
        DummyClient(stats), {"foo": "bar"}, "Producer"
    )
    payload = json.loads(collector.get_metrics_json())

    assert payload["client_type"] == "Producer"
    assert payload["client_version"] == "1.2.3"
    assert payload["foo"] == "bar"
    assert payload["state"] == "stable"
    assert payload["client_name"] == "client"
    assert payload["cgrp_state"] == "stable"
    assert payload["ActionDate"] == 1700000000
    assert payload["topic_names"] == "topic_a,topic_b"
    assert isinstance(payload["topics"], list)
    assert all("partitions" not in topic for topic in payload["topics"])
    assert len(payload["metrics_id"]) == 32


def test_metrics_collector_config_bool_lowercase(monkeypatch):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))

    collector = red_metrics.MetricsCollector(
        DummyClient({}),
        {
            "enable.partition.eof": False,
            "enable.auto.commit": True,
            "retries": 3,
            "skip.none": None,
        },
        "Consumer",
    )
    payload = json.loads(collector.get_metrics_json())

    assert payload["enable.partition.eof"] == "false"
    assert payload["enable.auto.commit"] == "true"
    assert payload["retries"] == "3"
    assert "skip.none" not in payload


class DummyClientWithDump(DummyClient):
    def __init__(self, stats, dumped_conf):
        super().__init__(stats)
        self._dumped_conf = dumped_conf

    def config_dump(self):
        return self._dumped_conf


class DummyClientWithBrokenDump(DummyClient):
    def config_dump(self):
        raise RuntimeError("boom")


def test_metrics_collector_prefers_client_config_dump(monkeypatch):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))

    collector = red_metrics.MetricsCollector(
        DummyClientWithDump({}, {
            "enable.auto.commit": "true",
            "fetch.max.bytes": "52428800",
            "opaque": "0xdeadbeef",
            "cluster.name": "cluster-a",
            "original.metadata.broker.list": "eds://kafka-1",
        }),
        {
            "enable.auto.commit": False,
            "local.only": "x",
        },
        "Consumer",
    )

    payload = json.loads(collector.get_metrics_json())
    assert payload["enable.auto.commit"] == "true"
    assert payload["fetch.max.bytes"] == "52428800"
    assert payload["cluster_name"] == "cluster-a"
    assert payload["original_metadata_broker_list"] == "eds://kafka-1"
    assert "opaque" not in payload
    assert "local.only" not in payload
    assert payload["group.id"] == ""
    assert payload["statistics.interval.ms"] == ""


def test_metrics_collector_redacts_sensitive_config(monkeypatch):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))

    collector = red_metrics.MetricsCollector(
        DummyClientWithDump({}, {
            "sasl.username": "user",
            "sasl.password": "secret-pass",
            "sasl.jaas.config": 'required username="user" password="secret";',
            "ssl.key.location": "/path/to/private.key",
            "client.id": "client-a",
        }),
        {},
        "Producer",
    )

    payload = json.loads(collector.get_metrics_json())
    assert payload["sasl.username"] == "user"
    assert payload["sasl.password"] == red_metrics._REDACTED_CONFIG_VALUE
    assert payload["sasl.jaas.config"] == red_metrics._REDACTED_CONFIG_VALUE
    assert payload["ssl.key.location"] == red_metrics._REDACTED_CONFIG_VALUE
    assert payload["client.id"] == "client-a"


def test_metrics_collector_fallback_to_conf_on_dump_error(monkeypatch):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))

    collector = red_metrics.MetricsCollector(
        DummyClientWithBrokenDump({}),
        {
            "enable.partition.eof": False,
            "statistics.interval.ms": 10000,
        },
        "Consumer",
    )

    payload = json.loads(collector.get_metrics_json())
    assert payload["enable.partition.eof"] == "false"
    assert payload["statistics.interval.ms"] == "10000"


def test_metrics_collector_logs_fallback_to_constructor_config(monkeypatch, caplog):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))
    caplog.set_level(logging.ERROR, logger="confluent_kafka.red_metrics")

    collector = red_metrics.MetricsCollector(
        DummyClientWithBrokenDump({}),
        {"statistics.interval.ms": 10000},
        "Consumer",
    )
    payload = json.loads(collector.get_metrics_json())

    assert payload["statistics.interval.ms"] == "10000"
    assert "config_dump failed, falling back to constructor config" in caplog.text
    assert "client_type=Consumer" in caplog.text


def test_metrics_sender_logs_disabled_when_url_missing(monkeypatch, caplog):
    monkeypatch.setattr(red_metrics.EnvUtil, "_config_env", "")
    monkeypatch.setattr(red_metrics.EnvUtil, "_job_env", "")
    monkeypatch.setattr(red_metrics.EnvUtil, "_xhs_env", "")
    caplog.set_level(logging.INFO, logger="confluent_kafka.red_metrics")

    sender = red_metrics.MetricsSender(DummyClient({}), {}, "Producer", interval=1)

    assert sender.start() is False
    assert "metrics sender disabled" in caplog.text
    assert "reason=no_collect_url" in caplog.text


def test_metrics_sender_logs_send_attempt_without_url_query(monkeypatch, caplog):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))
    monkeypatch.setattr(
        red_metrics.urllib.request,
        "urlopen",
        lambda req, timeout: DummyUrlopenResponse(),
    )
    caplog.set_level(logging.DEBUG, logger="confluent_kafka.red_metrics")

    sender = red_metrics.MetricsSender(
        DummyClient({"time": 1700000000}),
        {"metrics.collect.url": "http://collector.local/api?token=secret-token"},
        "Producer",
        interval=1,
    )
    sender._do_task()

    assert "metrics send attempt" in caplog.text
    assert "metrics send completed" in caplog.text
    assert "http://collector.local/api" in caplog.text
    assert "secret-token" not in caplog.text


def test_cimpl_config_dump_smoke():
    producer = Producer({"bootstrap.servers": "127.0.0.1:1"})
    dumped = producer.config_dump()

    assert isinstance(dumped, dict)
    assert dumped.get("metadata.broker.list") == "127.0.0.1:1"

    producer.flush(0)


def test_stats_collect_after_consumer_close_raises_runtime_error():
    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:1",
        "group.id": "closed-stats-collect",
    })
    consumer.close()

    with pytest.raises(RuntimeError, match="Client has been closed"):
        consumer.stats_collect()


def test_metrics_collector_required_fragmentation_fields_present(monkeypatch):
    monkeypatch.setattr(red_metrics, "libversion", lambda: ("1.2.3", 0x010203))

    collector = red_metrics.MetricsCollector(
        DummyClientWithDump({}, {"enable.auto.commit": "true"}),
        {},
        "Consumer",
    )
    payload = json.loads(collector.get_metrics_json())

    missing = sorted(k for k in red_metrics._REQUIRED_FRAGMENTATION_FIELDS if k not in payload)
    assert not missing
