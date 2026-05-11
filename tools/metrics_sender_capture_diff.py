#!/usr/bin/env python3
import argparse
import datetime
import gzip
import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_IGNORE_HEADERS = {
    "accept-encoding",
    "connection",
    "content-length",
    "date",
    "host",
    "user-agent",
}
DEFAULT_IGNORE_KEYS = {"metrics_id"}


def utc_now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def json_type_name(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def decode_request_body(headers, raw_body):
    encoding = (headers or {}).get("content-encoding", "")
    encoding = encoding.lower()
    body = raw_body
    error = None
    if "gzip" in encoding:
        try:
            body = gzip.decompress(raw_body)
        except Exception as exc:
            error = "gzip_decompress_failed: %s" % (exc,)
            return raw_body, error
    return body, error


def normalize_headers_map(headers, ignore_headers=None):
    ignore_headers = set(h.lower() for h in (ignore_headers or set()))
    normalized = {}
    for key, value in (headers or {}).items():
        key_l = key.lower()
        if key_l in ignore_headers:
            continue
        normalized[key_l] = value
    return dict(sorted(normalized.items()))


def _looks_like_topics_list(value):
    if not isinstance(value, list) or not value:
        return False
    for item in value:
        if not isinstance(item, dict):
            return False
        if "topic_name" not in item:
            return False
    return True


def normalize_payload(value, ignore_keys=None, ignore_paths=None, path="$"):
    ignore_keys = set(ignore_keys or set())
    ignore_paths = set(ignore_paths or set())

    if path in ignore_paths:
        return _Ignored

    if isinstance(value, dict):
        out = {}
        for key in sorted(value.keys()):
            child_path = "%s.%s" % (path, key)
            if key in ignore_keys or child_path in ignore_paths:
                continue
            child = normalize_payload(
                value[key],
                ignore_keys=ignore_keys,
                ignore_paths=ignore_paths,
                path=child_path,
            )
            if child is _Ignored:
                continue
            out[key] = child
        return out

    if isinstance(value, list):
        items = []
        for idx, item in enumerate(value):
            child_path = "%s[%d]" % (path, idx)
            child = normalize_payload(
                item,
                ignore_keys=ignore_keys,
                ignore_paths=ignore_paths,
                path=child_path,
            )
            if child is _Ignored:
                continue
            items.append(child)
        if _looks_like_topics_list(items):
            items = sorted(items, key=lambda item: str(item.get("topic_name", "")))
        return items

    return value


class _IgnoredType:
    pass


_Ignored = _IgnoredType()


def _empty_diff_result():
    return {
        "only_in_py": [],
        "only_in_cpp": [],
        "type_mismatches": [],
        "value_mismatches": [],
    }


def diff_json(py_value, cpp_value, path="$", out=None):
    if out is None:
        out = _empty_diff_result()

    py_type = json_type_name(py_value)
    cpp_type = json_type_name(cpp_value)

    if py_type != cpp_type:
        out["type_mismatches"].append(
            {
                "path": path,
                "py_type": py_type,
                "cpp_type": cpp_type,
                "py_value": py_value,
                "cpp_value": cpp_value,
            }
        )
        return out

    if isinstance(py_value, dict):
        py_keys = set(py_value.keys())
        cpp_keys = set(cpp_value.keys())
        for key in sorted(py_keys - cpp_keys):
            out["only_in_py"].append(
                {"path": "%s.%s" % (path, key), "value": py_value[key], "type": json_type_name(py_value[key])}
            )
        for key in sorted(cpp_keys - py_keys):
            out["only_in_cpp"].append(
                {"path": "%s.%s" % (path, key), "value": cpp_value[key], "type": json_type_name(cpp_value[key])}
            )
        for key in sorted(py_keys & cpp_keys):
            diff_json(py_value[key], cpp_value[key], path="%s.%s" % (path, key), out=out)
        return out

    if isinstance(py_value, list):
        py_len = len(py_value)
        cpp_len = len(cpp_value)
        if py_len != cpp_len:
            out["value_mismatches"].append(
                {"path": path, "kind": "array_length", "py_value": py_len, "cpp_value": cpp_len}
            )
        for idx in range(min(py_len, cpp_len)):
            diff_json(py_value[idx], cpp_value[idx], path="%s[%d]" % (path, idx), out=out)
        return out

    if py_value != cpp_value:
        out["value_mismatches"].append(
            {"path": path, "kind": "scalar", "py_value": py_value, "cpp_value": cpp_value}
        )
    return out


def diff_headers(py_headers, cpp_headers):
    out = _empty_diff_result()
    py_keys = set(py_headers.keys())
    cpp_keys = set(cpp_headers.keys())
    for key in sorted(py_keys - cpp_keys):
        out["only_in_py"].append({"path": key, "value": py_headers[key], "type": "string"})
    for key in sorted(cpp_keys - py_keys):
        out["only_in_cpp"].append({"path": key, "value": cpp_headers[key], "type": "string"})
    for key in sorted(py_keys & cpp_keys):
        if py_headers[key] != cpp_headers[key]:
            out["value_mismatches"].append(
                {"path": key, "kind": "header", "py_value": py_headers[key], "cpp_value": cpp_headers[key]}
            )
    return out


def _truncate_items(items, max_items):
    if max_items is None or max_items < 0:
        return list(items), 0
    shown = list(items[:max_items])
    hidden = max(len(items) - len(shown), 0)
    return shown, hidden


def _format_json_inline(value):
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return repr(value)


def _append_diff_section(lines, title, diff_result, max_items):
    lines.append("## %s" % title)
    lines.append("")
    for bucket in ("only_in_py", "only_in_cpp", "type_mismatches", "value_mismatches"):
        items = diff_result.get(bucket, [])
        lines.append("- `%s`: %d" % (bucket, len(items)))
    lines.append("")

    for bucket in ("only_in_py", "only_in_cpp", "type_mismatches", "value_mismatches"):
        items = diff_result.get(bucket, [])
        shown, hidden = _truncate_items(items, max_items)
        lines.append("### `%s`" % bucket)
        if not shown:
            lines.append("")
            lines.append("- (empty)")
            lines.append("")
            continue
        lines.append("")
        for item in shown:
            path = item.get("path", "")
            if bucket == "type_mismatches":
                lines.append(
                    "- `%s`: py=`%s`, cpp=`%s`" % (path, item.get("py_type"), item.get("cpp_type"))
                )
                continue
            if bucket == "value_mismatches":
                lines.append(
                    "- `%s`: py=%s ; cpp=%s"
                    % (path, _format_json_inline(item.get("py_value")), _format_json_inline(item.get("cpp_value")))
                )
                continue
            lines.append("- `%s` (%s)" % (path, item.get("type")))
        if hidden:
            lines.append("- ... (%d more)" % hidden)
        lines.append("")


def compare_capture_records(
    py_record,
    cpp_record,
    ignore_keys=None,
    ignore_paths=None,
    ignore_headers=None,
):
    py_headers = normalize_headers_map(py_record.get("headers"), ignore_headers=ignore_headers)
    cpp_headers = normalize_headers_map(cpp_record.get("headers"), ignore_headers=ignore_headers)

    py_payload = py_record.get("body_json")
    cpp_payload = cpp_record.get("body_json")
    py_payload_norm = normalize_payload(py_payload, ignore_keys=ignore_keys, ignore_paths=ignore_paths)
    cpp_payload_norm = normalize_payload(cpp_payload, ignore_keys=ignore_keys, ignore_paths=ignore_paths)

    header_diff = diff_headers(py_headers, cpp_headers)
    payload_diff = diff_json(py_payload_norm, cpp_payload_norm)

    py_top = py_payload_norm if isinstance(py_payload_norm, dict) else {}
    cpp_top = cpp_payload_norm if isinstance(cpp_payload_norm, dict) else {}
    top_level_diff = diff_json(py_top, cpp_top, path="$")

    return {
        "py_headers": py_headers,
        "cpp_headers": cpp_headers,
        "py_payload": py_payload_norm,
        "cpp_payload": cpp_payload_norm,
        "header_diff": header_diff,
        "payload_diff": payload_diff,
        "top_level_diff": top_level_diff,
    }


def build_markdown_report(
    compare_result,
    py_record,
    cpp_record,
    ignore_keys=None,
    ignore_paths=None,
    ignore_headers=None,
    max_items=200,
    input_paths=None,
):
    lines = []
    lines.append("# MetricsSender Runtime Payload Diff Report")
    lines.append("")
    lines.append("- Generated at: `%s`" % utc_now_iso())
    if input_paths:
        lines.append("- Inputs: `%s`" % "`, `".join(input_paths))
    lines.append("- Python record path: `%s`" % py_record.get("path", ""))
    lines.append("- C++ record path: `%s`" % cpp_record.get("path", ""))
    lines.append("- Python captured_at: `%s`" % py_record.get("captured_at", ""))
    lines.append("- C++ captured_at: `%s`" % cpp_record.get("captured_at", ""))
    lines.append("- Ignored payload keys: `%s`" % ", ".join(sorted(ignore_keys or [])))
    lines.append("- Ignored payload paths: `%s`" % ", ".join(sorted(ignore_paths or [])))
    lines.append("- Ignored headers: `%s`" % ", ".join(sorted(ignore_headers or [])))
    lines.append("")

    _append_diff_section(lines, "Header Diff", compare_result["header_diff"], max_items)
    _append_diff_section(lines, "Payload Top-Level Diff", compare_result["top_level_diff"], max_items)
    _append_diff_section(lines, "Payload Recursive Diff", compare_result["payload_diff"], max_items)
    return "\n".join(lines) + "\n"


def _append_jsonl(path, record):
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True))
        fh.write("\n")


def _load_jsonl(path):
    records = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception as exc:
                raise ValueError("invalid JSONL at %s:%d: %s" % (path, line_no, exc))
    return records


def _filter_records(records, request_path):
    if request_path is None:
        return list(records)
    return [record for record in records if record.get("path") == request_path]


def _select_record(records, label, request_path=None, index=-1):
    filtered = _filter_records(records, request_path=request_path)
    filtered = [record for record in filtered if isinstance(record.get("body_json"), (dict, list))]
    if not filtered:
        path_msg = request_path if request_path is not None else "<any>"
        raise ValueError("no JSON payload record found for %s (path=%s)" % (label, path_msg))
    try:
        return filtered[index]
    except IndexError:
        raise ValueError(
            "record index out of range for %s: index=%s available=%d" % (label, index, len(filtered))
        )


def _make_capture_record(handler, raw_body):
    headers = {k.lower(): v for k, v in handler.headers.items()}
    decoded_body, decode_error = decode_request_body(headers, raw_body)

    body_text = None
    body_json = None
    json_error = None
    try:
        body_text = decoded_body.decode("utf-8")
    except Exception as exc:
        json_error = "utf8_decode_failed: %s" % (exc,)
    else:
        try:
            body_json = json.loads(body_text)
        except Exception as exc:
            json_error = "json_parse_failed: %s" % (exc,)

    return {
        "captured_at": utc_now_iso(),
        "method": handler.command,
        "path": handler.path,
        "remote_addr": handler.client_address[0] if handler.client_address else "",
        "headers": headers,
        "raw_body_len": len(raw_body),
        "decoded_body_len": len(decoded_body),
        "body_sha256": hashlib.sha256(decoded_body).hexdigest(),
        "body_text": body_text,
        "body_json": body_json,
        "decode_error": decode_error,
        "json_error": json_error,
    }


class _CaptureServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, output_path, max_requests=None):
        ThreadingHTTPServer.__init__(self, server_address, _CaptureHandler)
        self.output_path = output_path
        self.max_requests = max_requests
        self.request_count = 0
        self.request_count_lock = threading.Lock()


class _CaptureHandler(BaseHTTPRequestHandler):
    server_version = "MetricsCapture/1.0"

    def log_message(self, fmt, *args):
        return

    def _send_json(self, status, payload):
        data = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/healthz":
            self._send_json(200, {"ok": True})
            return
        self._send_json(200, {"ok": True, "hint": "POST metrics payload to capture"})

    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0
        raw_body = self.rfile.read(content_length)
        record = _make_capture_record(self, raw_body)
        _append_jsonl(self.server.output_path, record)

        shutdown_needed = False
        with self.server.request_count_lock:
            self.server.request_count += 1
            if self.server.max_requests and self.server.request_count >= self.server.max_requests:
                shutdown_needed = True

        self._send_json(
            200,
            {
                "ok": True,
                "captured": self.server.request_count,
                "output": str(self.server.output_path),
                "path": self.path,
            },
        )

        if shutdown_needed:
            threading.Thread(target=self.server.shutdown, daemon=True).start()


def run_capture_server(args):
    output_path = Path(args.output).resolve()
    server = _CaptureServer((args.bind, args.port), output_path=output_path, max_requests=args.max_requests)
    print("Capture server listening on http://%s:%d" % (args.bind, args.port))
    print("Writing JSONL to %s" % output_path)
    if args.max_requests:
        print("Will stop after %d POST requests" % args.max_requests)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        print("Capture server stopped. Captured %d request(s)." % server.request_count)


def run_report(args):
    if args.input:
        py_records = _load_jsonl(args.input)
        cpp_records = py_records
        input_paths = [str(Path(args.input))]
    else:
        py_records = _load_jsonl(args.py_input)
        cpp_records = _load_jsonl(args.cpp_input)
        input_paths = [str(Path(args.py_input)), str(Path(args.cpp_input))]

    ignore_keys = set(DEFAULT_IGNORE_KEYS)
    ignore_keys.update(args.ignore_key or [])
    ignore_paths = set(args.ignore_path or [])

    ignore_headers = set(DEFAULT_IGNORE_HEADERS)
    ignore_headers.update([h.lower() for h in (args.ignore_header or [])])

    py_record = _select_record(py_records, "python", request_path=args.py_path, index=args.py_index)
    cpp_record = _select_record(cpp_records, "cpp", request_path=args.cpp_path, index=args.cpp_index)

    compare_result = compare_capture_records(
        py_record,
        cpp_record,
        ignore_keys=ignore_keys,
        ignore_paths=ignore_paths,
        ignore_headers=ignore_headers,
    )
    report = build_markdown_report(
        compare_result,
        py_record,
        cpp_record,
        ignore_keys=ignore_keys,
        ignore_paths=ignore_paths,
        ignore_headers=ignore_headers,
        max_items=args.max_items,
        input_paths=input_paths,
    )

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print("Wrote report to %s" % out_path)
    else:
        print(report)


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Capture MetricsSender HTTP payloads and generate Python vs C++ diff reports."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run a local HTTP collector that captures POST payloads to JSONL.")
    serve.add_argument("--bind", default="127.0.0.1", help="Bind address.")
    serve.add_argument("--port", type=int, default=18080, help="Listen port.")
    serve.add_argument("--output", required=True, help="JSONL file path for captured requests.")
    serve.add_argument("--max-requests", type=int, default=None, help="Stop automatically after N POSTs.")
    serve.set_defaults(func=run_capture_server)

    report = sub.add_parser("report", help="Generate Markdown diff report from captured JSONL.")
    report_group = report.add_mutually_exclusive_group(required=True)
    report_group.add_argument("--input", help="Single JSONL file containing both Python and C++ captures.")
    report_group.add_argument(
        "--py-input", help="Python capture JSONL file. Requires --cpp-input when using split inputs."
    )
    report.add_argument("--cpp-input", help="C++ capture JSONL file when using split inputs.")
    report.add_argument("--py-path", required=True, help="Request path used by Python capture, e.g. /py")
    report.add_argument("--cpp-path", required=True, help="Request path used by C++ capture, e.g. /cpp")
    report.add_argument("--py-index", type=int, default=-1, help="Record index within filtered Python captures.")
    report.add_argument("--cpp-index", type=int, default=-1, help="Record index within filtered C++ captures.")
    report.add_argument("--ignore-key", action="append", default=[], help="Payload key name to ignore at any level.")
    report.add_argument("--ignore-path", action="append", default=[], help="Exact JSON path to ignore, e.g. $.time")
    report.add_argument("--ignore-header", action="append", default=[], help="HTTP header name to ignore.")
    report.add_argument("--max-items", type=int, default=200, help="Max items per diff bucket in Markdown output.")
    report.add_argument("--output", help="Write Markdown report to file instead of stdout.")
    report.set_defaults(func=run_report)

    return parser


def main():
    parser = _build_parser()
    args = parser.parse_args()
    if args.command == "report" and args.py_input and not args.cpp_input:
        parser.error("--cpp-input is required when --py-input is used")
    args.func(args)


if __name__ == "__main__":
    main()
