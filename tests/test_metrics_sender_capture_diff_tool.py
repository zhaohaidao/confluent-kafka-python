import gzip
import importlib.util
import pathlib


def _load_tool_module():
    root = pathlib.Path(__file__).resolve().parents[1]
    module_path = root / "tools" / "metrics_sender_capture_diff.py"
    spec = importlib.util.spec_from_file_location("metrics_sender_capture_diff", str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


tool = _load_tool_module()


def test_decode_request_body_gzip():
    raw = gzip.compress(b'{"k":1}')
    decoded, error = tool.decode_request_body({"content-encoding": "gzip"}, raw)

    assert error is None
    assert decoded == b'{"k":1}'


def test_normalize_payload_ignores_keys_paths_and_sorts_topics():
    payload = {
        "metrics_id": "abc",
        "time": 123,
        "topics": [
            {"topic_name": "b", "value": 2},
            {"topic_name": "a", "value": 1},
        ],
        "nested": {"keep": 1, "drop": 2},
    }

    normalized = tool.normalize_payload(
        payload,
        ignore_keys={"metrics_id"},
        ignore_paths={"$.nested.drop", "$.time"},
    )

    assert "metrics_id" not in normalized
    assert "time" not in normalized
    assert normalized["topics"][0]["topic_name"] == "a"
    assert normalized["topics"][1]["topic_name"] == "b"
    assert normalized["nested"] == {"keep": 1}


def test_diff_json_reports_missing_type_and_value_mismatches():
    py_payload = {
        "same": 1,
        "only_py": True,
        "type_diff": 1,
        "nested": {"a": 1},
        "list": [1, 2],
    }
    cpp_payload = {
        "same": 2,
        "only_cpp": False,
        "type_diff": "1",
        "nested": {"b": 2},
        "list": [1],
    }

    diff = tool.diff_json(py_payload, cpp_payload)

    assert any(item["path"] == "$.only_py" for item in diff["only_in_py"])
    assert any(item["path"] == "$.only_cpp" for item in diff["only_in_cpp"])
    assert any(item["path"] == "$.type_diff" for item in diff["type_mismatches"])
    assert any(item["path"] == "$.same" for item in diff["value_mismatches"])
    assert any(item["path"] == "$.list" and item["kind"] == "array_length" for item in diff["value_mismatches"])
    assert any(item["path"] == "$.nested.a" for item in diff["only_in_py"])
    assert any(item["path"] == "$.nested.b" for item in diff["only_in_cpp"])


def test_compare_capture_records_diffs_headers_and_payload():
    py_record = {
        "headers": {"Biz-Type": "kafka_python_sdk_config_and_metrics", "Content-Length": "10"},
        "body_json": {"metrics_id": "a", "foo": 1},
        "path": "/py",
    }
    cpp_record = {
        "headers": {"Biz-Type": "kafka_cpp_sdk_config_and_metrics", "Content-Length": "20"},
        "body_json": {"metrics_id": "b", "foo": "1", "bar": 2},
        "path": "/cpp",
    }

    result = tool.compare_capture_records(
        py_record,
        cpp_record,
        ignore_keys={"metrics_id"},
        ignore_headers={"content-length"},
    )

    assert result["py_headers"] == {"biz-type": "kafka_python_sdk_config_and_metrics"}
    assert result["cpp_headers"] == {"biz-type": "kafka_cpp_sdk_config_and_metrics"}
    assert any(item["path"] == "biz-type" for item in result["header_diff"]["value_mismatches"])
    assert any(item["path"] == "$.bar" for item in result["payload_diff"]["only_in_cpp"])
    assert any(item["path"] == "$.foo" for item in result["payload_diff"]["type_mismatches"])
