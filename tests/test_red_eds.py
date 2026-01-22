import pytest

from confluent_kafka import red_eds


class DummyEdsClient:
    def __init__(self, instances):
        self._instances = instances
        self.calls = []

    def get_instances(self, service_name):
        self.calls.append(service_name)
        return self._instances


def _set_env(monkeypatch):
    monkeypatch.setenv("XHS_ENV", "staging")
    monkeypatch.setenv("XHS_SERVICE", "svc")
    monkeypatch.setenv("XHS_REGION", "qc-sh")
    monkeypatch.setenv("XHS_ZONE", "qcsh5")
    monkeypatch.setenv("EDS_HTTP_HOST", "10.0.0.1:8085")


def test_resolve_eds_bootstrap(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(
        [
            {"address": "10.0.0.1:9092"},
            {"host": "10.0.0.2", "port": 9092},
        ]
    )
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest"]
    assert resolved["bootstrap.servers"] == "10.0.0.1:9092,10.0.0.2:9092"
    assert resolved["original.metadata.broker.list"] == "eds://kafka-eds-paastest"


def test_resolve_eds_service_name(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9092"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {"kafka.service.name": "kafka-eds-paastest"}
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9092"
    assert "kafka.service.name" not in resolved


def test_resolve_eds_missing_env(monkeypatch):
    for key in red_eds._REQUIRED_ENV_VARS:
        monkeypatch.delenv(key, raising=False)

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    with pytest.raises(red_eds.EdsResolveError):
        red_eds.resolve_eds_bootstrap(conf)
