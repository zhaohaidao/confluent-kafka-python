import csv
import importlib.util
import sys
import types
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO_ROOT / "tools" / "run_auth_csv_tests.py"
JAVA_TOOL_PATH = REPO_ROOT / "tools" / "run_auth_csv_tests_java.py"
CHECK_TOOL_PATH = REPO_ROOT / "tools" / "check_auth_csv_results.py"


def load_tool_module():
    spec = importlib.util.spec_from_file_location("auth_csv_runner_tool", TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_java_tool_module():
    spec = importlib.util.spec_from_file_location("auth_csv_runner_java_tool", JAVA_TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_check_tool_module():
    spec = importlib.util.spec_from_file_location("auth_csv_result_check_tool", CHECK_TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_account_pair():
    tool = load_tool_module()
    username, password = tool.parse_account_pair("user_a=pass_a")
    assert username == "user_a"
    assert password == "=pass_a"


def test_java_runner_redacts_password_in_recorded_command():
    tool = load_java_tool_module()

    redacted = tool.redact_command([
        "java",
        "AuthScenarioCli",
        "--username",
        "user_a",
        "--password",
        "secret",
    ])

    assert redacted == [
        "java",
        "AuthScenarioCli",
        "--username",
        "user_a",
        "--password",
        "[redacted]",
    ]


def test_load_auth_cases_with_gb2312(tmp_path):
    tool = load_tool_module()
    csv_path = tmp_path / "auth.csv"
    fieldnames = [
        tool.FIELD_TOPIC,
        tool.FIELD_GROUP,
        tool.FIELD_ACCOUNT,
        tool.FIELD_PYTHON_EFFECT,
    ]
    row = {
        tool.FIELD_TOPIC: "topic_a",
        tool.FIELD_GROUP: "group_a",
        tool.FIELD_ACCOUNT: "user_a=pass_a",
        tool.FIELD_PYTHON_EFFECT: "",
    }
    with open(csv_path, "w", encoding="gb2312", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(row)

    cases, loaded_fields, encoding = tool.load_auth_cases(str(csv_path))
    assert encoding == "gb2312"
    assert loaded_fields == fieldnames
    assert len(cases) == 1
    assert cases[0].topic == "topic_a"
    assert cases[0].group == "group_a"
    assert cases[0].account_pair == "user_a=pass_a"


def test_run_case_dry_run():
    tool = load_tool_module()
    args = types.SimpleNamespace(
        bootstrap="127.0.0.1:9092",
        anonymous_bootstrap="127.0.0.1:9092",
        dry_run=True,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanisms="PLAIN",
        anonymous_security_protocol="",
        socket_timeout_ms=2000,
        message_timeout_ms=3000,
        flush_timeout_sec=1.0,
        consume_timeout_sec=1.0,
        poll_timeout_sec=0.1,
    )
    case_obj = tool.AuthCase(
        topic="topic_a",
        group="group_a",
        account_pair="user_a=pass_a",
        raw_row={
            tool.FIELD_TOPIC: "topic_a",
            tool.FIELD_GROUP: "group_a",
            tool.FIELD_ACCOUNT: "user_a=pass_a",
            tool.FIELD_PYTHON_EFFECT: "",
        },
    )
    summary, details = tool.run_case(args, case_obj)
    assert "no_group_anonymous" in summary
    assert "with_group_authenticated" in summary
    assert summary.count("p=ok,c=ok") == 4
    assert set(details.keys()) == {
        "no_group_anonymous",
        "no_group_authenticated",
        "with_group_anonymous",
        "with_group_authenticated",
    }


def test_build_jaas_uses_scram_login_module():
    tool = load_tool_module()
    jaas = tool.build_jaas("u", "p", "SCRAM-SHA-256")
    assert "security.scram.ScramLoginModule" in jaas
    assert 'username="u"' in jaas
    assert 'password="p"' in jaas


def test_build_jaas_uses_plain_login_module():
    tool = load_tool_module()
    jaas = tool.build_jaas("u", "p", "PLAIN")
    assert "security.plain.PlainLoginModule" in jaas
    assert 'username="u"' in jaas
    assert 'password="p"' in jaas


def test_build_jaas_preserves_special_password_chars():
    tool = load_tool_module()
    password = '=dWRmX2Rl+-/ZmF1bHQ'
    jaas = tool.build_jaas("udf_default_201", password, "SCRAM-SHA-256")
    assert 'username="udf_default_201"' in jaas
    assert 'password="%s"' % password in jaas


def test_build_scenario_conf_uses_anonymous_bootstrap():
    tool = load_tool_module()
    args = types.SimpleNamespace(
        bootstrap="10.0.0.1:9093",
        anonymous_bootstrap="10.0.0.2:9092",
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanisms="SCRAM-SHA-256",
        anonymous_security_protocol="",
        socket_timeout_ms=2000,
    )
    case_obj = tool.AuthCase(
        topic="topic_a",
        group="group_a",
        account_pair="user_a=pass_a",
        raw_row={},
    )

    anonymous_conf, _ = tool.build_scenario_conf(
        args, case_obj, use_group=False, use_auth=False
    )
    assert anonymous_conf["bootstrap.servers"] == "10.0.0.2:9092"
    assert anonymous_conf["auto.offset.reset"] == "latest"
    assert "security.protocol" not in anonymous_conf

    auth_conf, _ = tool.build_scenario_conf(
        args, case_obj, use_group=False, use_auth=True
    )
    assert auth_conf["bootstrap.servers"] == "10.0.0.1:9093"
    assert auth_conf["auto.offset.reset"] == "latest"
    assert auth_conf["security.protocol"] == "SASL_PLAINTEXT"


def test_run_produce_prefers_client_auth_error(monkeypatch):
    tool = load_tool_module()

    class FakeProducer:
        def __init__(self, conf):
            self._error_cb = conf.get("error_cb")

        def produce(self, topic, value, key, on_delivery):
            on_delivery(
                RuntimeError(
                    'KafkaError{code=_MSG_TIMED_OUT,val=-192,str="Local: Message timed out"}'
                ),
                None,
            )
            if self._error_cb is not None:
                self._error_cb(
                    RuntimeError(
                        'KafkaError{code=_AUTHENTICATION,val=-169,str="SASL authentication failure"}'
                    )
                )

        def poll(self, timeout):
            return None

        def flush(self, timeout):
            return 0

    monkeypatch.setitem(
        sys.modules,
        "confluent_kafka",
        types.SimpleNamespace(Producer=FakeProducer),
    )

    args = types.SimpleNamespace(flush_timeout_sec=1.0)
    case_obj = tool.AuthCase(
        topic="topic_a",
        group="group_a",
        account_pair="user_a=pass_a",
        raw_row={},
    )
    result = tool.run_produce(args, case_obj, {}, "marker")
    assert result.ok is False
    assert "_AUTHENTICATION" in result.detail


def test_check_auth_csv_results_passes_expected_matrix():
    tool = load_check_tool_module()
    row = {
        "topic": "topic_a",
        tool.FIELD_EXPECTED: (
            "写：实名：成功匿名：失败"
            "读：不带group+实名：失败不带group+匿名：失败"
            "带group+实名：成功带group+匿名：失败"
        ),
        tool.FIELD_PYTHON_EFFECT: (
            "no_group_anonymous[p=fail:denied,c=fail:skipped] | "
            "no_group_authenticated[p=ok,c=fail:denied] | "
            "with_group_anonymous[p=fail:denied,c=fail:skipped] | "
            "with_group_authenticated[p=ok,c=ok]"
        ),
    }

    assert tool.check_rows([row]) == []


def test_check_auth_csv_results_reports_mismatch():
    tool = load_check_tool_module()
    row = {
        "topic": "topic_a",
        tool.FIELD_EXPECTED: (
            "写：实名：成功匿名：成功"
            "读：不带group+实名：成功不带group+匿名：成功"
            "带group+实名：成功带group+匿名：成功"
        ),
        tool.FIELD_PYTHON_EFFECT: (
            "no_group_anonymous[p=ok,c=ok] | "
            "no_group_authenticated[p=ok,c=fail:denied] | "
            "with_group_anonymous[p=ok,c=ok] | "
            "with_group_authenticated[p=ok,c=ok]"
        ),
    }

    failures = tool.check_rows([row])
    assert failures == [
        {
            "row": 1,
            "topic": "topic_a",
            "scenario": "no_group_authenticated.c",
            "detail": "expected_ok_got_fail",
        }
    ]
