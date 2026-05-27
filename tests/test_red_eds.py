import logging

import pytest

from confluent_kafka import red_eds


class DummyEdsClient:
    def __init__(self, instances):
        self._instances = instances
        self.calls = []

    def get_instances(self, service_name):
        self.calls.append(service_name)
        return self._instances


class DummyHttpResponse:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


class DummyJsonHttpResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = ""

    def json(self):
        return self._payload


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


def test_resolve_eds_appends_sasl_suffix_when_security_protocol_is_set(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9093"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {
        "bootstrap.servers": "eds://kafka-eds-paastest",
        "security.protocol": "SASL_PLAINTEXT",
    }
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest-sasl"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9093"
    assert resolved["original.metadata.broker.list"] == "eds://kafka-eds-paastest"


def test_resolve_eds_ignores_security_protocol_env(monkeypatch):
    _set_env(monkeypatch)
    monkeypatch.setenv("kafka_security_protocol", "SASL_SSL")
    client = DummyEdsClient(["10.1.1.1:9092"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9092"
    assert resolved["original.metadata.broker.list"] == "eds://kafka-eds-paastest"


def test_resolve_eds_ignores_jaas_config_without_sasl_security_protocol(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9092"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {
        "bootstrap.servers": "eds://kafka-eds-paastest",
        "sasl.jaas.config": "login required;",
    }
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9092"
    assert resolved["original.metadata.broker.list"] == "eds://kafka-eds-paastest"


def test_resolve_eds_logs_route_and_redacts_sensitive_config(monkeypatch, caplog):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9093"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)
    caplog.set_level(logging.INFO, logger="confluent_kafka.red_eds")

    conf = {
        "bootstrap.servers": "eds://kafka-eds-paastest",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.jaas.config": 'login required password="secret-pass";',
        "group.id": "group-a",
        "metrics.collect.url": "http://collector.local/api?token=route-token",
    }
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert resolved["bootstrap.servers"] == "10.1.1.1:9093"
    assert "bootstrap resolution started" in caplog.text
    assert "bootstrap resolution completed" in caplog.text
    assert "security_enabled=True" in caplog.text
    assert "address_count=1" in caplog.text
    assert red_eds._REDACTED_CONFIG_VALUE in caplog.text
    assert "secret-pass" not in caplog.text
    assert "route-token" not in caplog.text


def test_resolve_eds_does_not_duplicate_sasl_suffix(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9093"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {
        "bootstrap.servers": "eds://kafka-eds-paastest-SASL",
        "security.protocol": "SASL_PLAINTEXT",
    }
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest-sasl"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9093"
    assert (
        resolved["original.metadata.broker.list"]
        == "eds://kafka-eds-paastest-SASL"
    )


def test_resolve_eds_appends_sasl_suffix_for_unhyphenated_sasl_suffix(monkeypatch):
    _set_env(monkeypatch)
    client = DummyEdsClient(["10.1.1.1:9093"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {
        "bootstrap.servers": "eds://kafka-eds-paastestSASL",
        "security.protocol": "SASL_PLAINTEXT",
    }
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastestSASL-sasl"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9093"
    assert (
        resolved["original.metadata.broker.list"]
        == "eds://kafka-eds-paastestSASL"
    )


def test_resolve_eds_missing_eds_http_host(monkeypatch):
    for key in red_eds._REQUIRED_ENV_VARS:
        monkeypatch.delenv(key, raising=False)

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    with pytest.raises(red_eds.EdsResolveError):
        red_eds.resolve_eds_bootstrap(conf)


def test_resolve_eds_logs_failure_context(monkeypatch, caplog):
    for key in red_eds._REQUIRED_ENV_VARS:
        monkeypatch.delenv(key, raising=False)
    caplog.set_level(logging.INFO, logger="confluent_kafka.red_eds")

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    with pytest.raises(red_eds.EdsResolveError):
        red_eds.resolve_eds_bootstrap(conf)

    assert "bootstrap resolution failed" in caplog.text
    assert "eds env missing" in caplog.text
    assert "kafka-eds-paastest" in caplog.text


def test_resolve_eds_requires_only_eds_http_host(monkeypatch):
    monkeypatch.setenv("EDS_HTTP_HOST", "10.0.0.1:8085")
    monkeypatch.delenv("XHS_ENV", raising=False)
    monkeypatch.delenv("XHS_SERVICE", raising=False)
    monkeypatch.delenv("XHS_REGION", raising=False)
    monkeypatch.delenv("XHS_ZONE", raising=False)
    client = DummyEdsClient(["10.1.1.1:9092"])
    monkeypatch.setattr(red_eds, "_create_eds_client", lambda: client)

    conf = {"bootstrap.servers": "eds://kafka-eds-paastest"}
    resolved = red_eds.resolve_eds_bootstrap(conf)

    assert client.calls == ["kafka-eds-paastest"]
    assert resolved["bootstrap.servers"] == "10.1.1.1:9092"
    assert resolved["original.metadata.broker.list"] == "eds://kafka-eds-paastest"


def test_resolve_cluster_bootstrap(monkeypatch):
    monkeypatch.setenv("JOB_ENV", "staging")
    called = {}

    def _fake_http_get(url, timeout):
        called["url"] = url
        called["timeout"] = timeout
        return DummyHttpResponse(
            200, '{"bootstrapStr":"10.0.0.2:9092,10.0.0.1:9092"}'
        )

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {"kafka.cluster.name": "kafka-main"}
    resolved = red_eds.resolve_bootstrap(conf)

    assert (
        called["url"]
        == "http://events.int.xiaohongshu.com/api/kmeta/cluster/kafka-main"
    )
    assert called["timeout"] == red_eds.KMETA_REQUEST_TIMEOUT_SECONDS
    assert resolved["bootstrap.servers"] == "10.0.0.2:9092,10.0.0.1:9092"
    assert resolved["original.metadata.broker.list"] == "cluster://kafka-main"
    assert "kafka.cluster.name" not in resolved


def test_resolve_cluster_logs_security_endpoint_decision(monkeypatch, caplog):
    monkeypatch.setenv("JOB_ENV", "staging")
    caplog.set_level(logging.INFO, logger="confluent_kafka.red_eds")

    def _fake_http_get(url, timeout):
        return DummyHttpResponse(200, "10.0.0.2:9093")

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {
        "kafka.cluster.name": "kafka-main",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.jaas.config": "login required password=secret;",
    }
    resolved = red_eds.resolve_bootstrap(conf)

    assert resolved["bootstrap.servers"] == "10.0.0.2:9093"
    assert "kmeta bootstrap request" in caplog.text
    assert red_eds.KMETA_SECURITY_BOOTSTRAP_API in caplog.text
    assert "security_reason=security.protocol" in caplog.text
    assert "password=secret" not in caplog.text


def test_builtin_eds_client_fetches_endpoints(monkeypatch):
    _set_env(monkeypatch)
    called = {}

    def _fake_http_get(url, timeout, params=None):
        called["url"] = url
        called["timeout"] = timeout
        called["params"] = params
        return DummyJsonHttpResponse(
            200,
            {
                "code": 0,
                "data": {
                    "version": "v1",
                    "endpoints": [
                        {"address": "10.0.0.1:9092"},
                        {"host": "10.0.0.2", "port": 9092},
                    ],
                },
            },
        )

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    client = red_eds._create_eds_client()
    instances = client.get_instances("kafka-eds-paastest")

    assert called["url"] == "http://10.0.0.1:8085/endpoints"
    assert called["timeout"] == 5
    assert called["params"] == {
        "clientName": "svc",
        "serviceName": "kafka-eds-paastest",
        "locality": "qcsh5",
        "apiVersion": "v1",
    }
    assert red_eds._normalize_addresses(instances) == [
        "10.0.0.1:9092",
        "10.0.0.2:9092",
    ]


def test_builtin_eds_client_omits_empty_optional_env(monkeypatch):
    monkeypatch.setenv("EDS_HTTP_HOST", "10.0.0.1:8085")
    monkeypatch.delenv("XHS_SERVICE", raising=False)
    monkeypatch.delenv("XHS_ZONE", raising=False)
    called = {}

    def _fake_http_get(url, timeout, params=None):
        called["url"] = url
        called["timeout"] = timeout
        called["params"] = params
        return DummyJsonHttpResponse(
            200,
            {
                "code": 0,
                "data": {
                    "endpoints": [{"address": "10.0.0.1:9092"}],
                },
            },
        )

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    instances = red_eds._create_eds_client().get_instances("kafka-eds-paastest")

    assert called["url"] == "http://10.0.0.1:8085/endpoints"
    assert called["params"] == {
        "serviceName": "kafka-eds-paastest",
        "apiVersion": "v1",
    }
    assert red_eds._normalize_addresses(instances) == ["10.0.0.1:9092"]


def test_cluster_security_bootstrap_enabled_by_conf(monkeypatch):
    monkeypatch.setenv("JOB_ENV", "prod")
    called = {}

    def _fake_http_get(url, timeout):
        called["url"] = url
        called["timeout"] = timeout
        return DummyHttpResponse(200, "10.1.1.1:9093")

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {
        "bootstrap.servers": "cluster://kafka-auth",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule "
            'required username="u" password="p";'
        ),
    }
    resolved = red_eds.resolve_bootstrap(conf)

    assert (
        called["url"]
        == "http://events.int.xiaohongshu.com/api/kmeta/cluster/security-bootstrap/kafka-auth"
    )
    assert called["timeout"] == red_eds.KMETA_REQUEST_TIMEOUT_SECONDS
    assert resolved["bootstrap.servers"] == "10.1.1.1:9093"
    assert resolved["original.metadata.broker.list"] == "cluster://kafka-auth"


def test_cluster_security_bootstrap_ignores_jaas_env(monkeypatch):
    monkeypatch.setenv("XHS_ENV", "sit")
    monkeypatch.setenv("kafka_sasl_jaas_config", "env-jaas")
    called = {}

    def _fake_http_get(url, timeout):
        called["url"] = url
        called["timeout"] = timeout
        return DummyHttpResponse(200, '"10.2.2.2:9093"')

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {"kafka.cluster.name": "kafka-auth-by-env"}
    resolved = red_eds.resolve_bootstrap(conf)

    assert (
        called["url"]
        == "http://events.int.sit.xiaohongshu.com/api/kmeta/cluster/kafka-auth-by-env"
    )
    assert called["timeout"] == red_eds.KMETA_REQUEST_TIMEOUT_SECONDS
    assert resolved["bootstrap.servers"] == "10.2.2.2:9093"


def test_cluster_bootstrap_rejects_plain_error_payload(monkeypatch):
    monkeypatch.setenv("JOB_ENV", "staging")

    def _fake_http_get(url, timeout):
        return DummyHttpResponse(200, "error:404")

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {"kafka.cluster.name": "missing-cluster"}
    with pytest.raises(red_eds.KmetaResolveError):
        red_eds.resolve_bootstrap(conf)


def test_cluster_bootstrap_rejects_json_error_payload(monkeypatch):
    monkeypatch.setenv("JOB_ENV", "staging")

    def _fake_http_get(url, timeout):
        return DummyHttpResponse(
            200,
            '{"code":1,"msg":"cluster not found","data":"error: cluster not found"}',
        )

    monkeypatch.setattr(red_eds, "_http_get", _fake_http_get)

    conf = {"kafka.cluster.name": "missing-cluster"}
    with pytest.raises(red_eds.KmetaResolveError):
        red_eds.resolve_bootstrap(conf)


def test_cluster_name_empty():
    conf = {"bootstrap.servers": "cluster://"}
    with pytest.raises(red_eds.KmetaResolveError):
        red_eds.resolve_bootstrap(conf)
