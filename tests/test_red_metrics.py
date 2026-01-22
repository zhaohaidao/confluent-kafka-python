import json

from confluent_kafka import Producer, red_metrics


class DummyClient:
    def __init__(self, stats):
        self._stats = stats

    def stats_collect(self):
        return json.dumps(self._stats)


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


def test_cimpl_config_dump_smoke():
    producer = Producer({"bootstrap.servers": "127.0.0.1:1"})
    dumped = producer.config_dump()

    assert isinstance(dumped, dict)
    assert dumped.get("metadata.broker.list") == "127.0.0.1:1"

    producer.flush(0)


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
