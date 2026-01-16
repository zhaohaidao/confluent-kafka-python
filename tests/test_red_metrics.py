import json

from confluent_kafka import red_metrics


class DummyClient:
    def __init__(self, stats):
        self._stats = stats

    def stats_collect(self):
        return json.dumps(self._stats)


def test_metrics_collector_basic(monkeypatch):
    stats = {
        "name": "client",
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
    assert payload["topic_names"] == "topic_a,topic_b"
    assert isinstance(payload["topics"], list)
    assert all("partitions" not in topic for topic in payload["topics"])
    assert len(payload["metrics_id"]) == 32
