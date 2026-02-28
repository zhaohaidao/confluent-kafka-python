#!/usr/bin/env python3
import argparse
import csv
import dataclasses
import glob
import json
import os
import sys
import time
import traceback
import uuid


DEFAULT_BOOTSTRAP = (
    "10.142.247.201:9093,10.142.247.204:9093,10.142.247.205:9093"
)
DEFAULT_ANONYMOUS_BOOTSTRAP = (
    "10.142.247.201:9092,10.142.247.204:9092,10.142.247.205:9092"
)
DEFAULT_CSV = "auth_tests.csv"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
for build_lib in sorted(glob.glob(os.path.join(REPO_ROOT, "build", "lib.*"))):
    if os.path.isdir(build_lib) and build_lib not in sys.path:
        sys.path.insert(0, build_lib)
if REPO_ROOT not in sys.path:
    sys.path.insert(1, REPO_ROOT)

FIELD_TOPIC = "topic"
FIELD_GROUP = "group"
FIELD_ACCOUNT = "\u8d26\u53f7\u5bc6\u7801"
FIELD_PYTHON_EFFECT = "python SDK \u6548\u679c"

SCENARIOS = (
    ("no_group_anonymous", False, False),
    ("no_group_authenticated", False, True),
    ("with_group_anonymous", True, False),
    ("with_group_authenticated", True, True),
)


@dataclasses.dataclass
class AuthCase:
    topic: str
    group: str
    account_pair: str
    raw_row: dict


@dataclasses.dataclass
class OperationResult:
    ok: bool
    detail: str


def parse_bool(value):
    return str(value or "").strip().upper() == "TRUE"


def load_auth_cases(csv_path):
    last_error = None
    for encoding in ("gb2312", "utf-8-sig", "utf-8"):
        try:
            with open(csv_path, "r", encoding=encoding, newline="") as fp:
                reader = csv.DictReader(fp)
                rows = []
                for raw in reader:
                    topic = (raw.get(FIELD_TOPIC) or "").strip()
                    group = (raw.get(FIELD_GROUP) or "").strip()
                    account_pair = (raw.get(FIELD_ACCOUNT) or "").strip()
                    if not topic:
                        continue
                    rows.append(AuthCase(topic, group, account_pair, raw))
                return rows, reader.fieldnames or [], encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("failed to read auth csv")


def parse_account_pair(account_pair):
    pair = (account_pair or "").strip()
    if not pair:
        raise ValueError("account pair is empty")
    if "=" not in pair:
        raise ValueError("account pair should be in username=password format")
    username, password = pair.split("=", 1)
    username = username.strip()
    password = password.strip()
    if not username:
        raise ValueError("account username is empty")
    if not password:
        raise ValueError("account password is empty")
    if not password.startswith("="):
        password = "=" + password
    return username, password


def get_login_module(sasl_mechanisms):
    mechanism = (sasl_mechanisms or "").strip().upper()
    if mechanism.startswith("SCRAM-"):
        return "org.apache.kafka.common.security.scram.ScramLoginModule"
    if mechanism == "PLAIN":
        return "org.apache.kafka.common.security.plain.PlainLoginModule"
    return "org.apache.kafka.common.security.plain.PlainLoginModule"


def build_jaas(username, password, sasl_mechanisms):
    esc_user = username.replace("\\", "\\\\").replace('"', '\\"')
    esc_pass = password.replace("\\", "\\\\").replace('"', '\\"')
    login_module = get_login_module(sasl_mechanisms)
    return (
        login_module + " "
        'required username="%s" password="%s";' % (esc_user, esc_pass)
    )


def short_error(exc):
    if exc is None:
        return ""
    text = str(exc).replace("\n", " ").strip()
    if len(text) > 160:
        return text[:157] + "..."
    return text


def build_base_conf(args):
    conf = {
        "bootstrap.servers": args.bootstrap,
        "socket.timeout.ms": args.socket_timeout_ms,
    }
    return conf


def build_auth_conf(args, account_pair):
    username, password = parse_account_pair(account_pair)
    return {
        "security.protocol": args.security_protocol,
        "sasl.mechanisms": args.sasl_mechanisms,
        "sasl.username": username,
        "sasl.password": password,
        "sasl.jaas.config": build_jaas(username, password, args.sasl_mechanisms),
    }


def make_group_id(case_obj, use_group):
    if use_group:
        return case_obj.group
    return "no-group-%s-%s" % (case_obj.topic, uuid.uuid4().hex[:8])


def run_produce(args, case_obj, conf, marker):
    from confluent_kafka import Producer

    state = {"error": None}

    def on_delivery(err, _msg):
        if err is not None and state["error"] is None:
            state["error"] = RuntimeError(str(err))

    try:
        producer = Producer(conf)
        producer.produce(
            case_obj.topic,
            value=marker.encode("utf-8"),
            key=marker.encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        remaining = producer.flush(args.flush_timeout_sec)
        if state["error"] is not None:
            return OperationResult(False, short_error(state["error"]))
        if remaining > 0:
            return OperationResult(False, "flush_timeout_unfinished=%d" % remaining)
        return OperationResult(True, "ok")
    except Exception as exc:
        return OperationResult(False, short_error(exc))


def wait_for_assignment(args, consumer):
    deadline = time.time() + args.assignment_timeout_sec
    while time.time() < deadline:
        msg = consumer.poll(timeout=args.poll_timeout_sec)
        if msg is not None and msg.error():
            return OperationResult(False, short_error(msg.error()))
        if consumer.assignment():
            return OperationResult(True, "ok")
    return OperationResult(False, "assignment_timeout")


def run_consume(args, case_obj, conf, probe_produce_conf, marker):
    from confluent_kafka import Consumer

    consumer = None
    try:
        consumer = Consumer(conf)
        consumer.subscribe([case_obj.topic])
        assignment_result = wait_for_assignment(args, consumer)
        if not assignment_result.ok:
            return assignment_result

        probe_produce_result = run_produce(
            args, case_obj, probe_produce_conf, marker
        )
        if not probe_produce_result.ok:
            return OperationResult(
                False, "consume_probe_failed:%s" % probe_produce_result.detail
            )

        deadline = time.time() + args.consume_timeout_sec
        while time.time() < deadline:
            msg = consumer.poll(timeout=args.poll_timeout_sec)
            if msg is None:
                continue
            if msg.error():
                return OperationResult(False, short_error(msg.error()))
            value = msg.value()
            if value is None:
                continue
            if value.decode("utf-8", errors="replace") == marker:
                return OperationResult(True, "ok")
        return OperationResult(False, "consume_timeout")
    except Exception as exc:
        return OperationResult(False, short_error(exc))
    finally:
        if consumer is not None:
            try:
                consumer.close()
            except Exception:
                pass


def build_scenario_conf(args, case_obj, use_group, use_auth):
    scenario_conf = build_base_conf(args)
    group_id = make_group_id(case_obj, use_group)
    scenario_conf["group.id"] = group_id
    scenario_conf["auto.offset.reset"] = "latest"
    scenario_conf["enable.auto.commit"] = False

    if use_auth:
        scenario_conf.update(build_auth_conf(args, case_obj.account_pair))
    else:
        if args.anonymous_bootstrap:
            scenario_conf["bootstrap.servers"] = args.anonymous_bootstrap
        if args.anonymous_security_protocol:
            scenario_conf["security.protocol"] = args.anonymous_security_protocol

    return scenario_conf, group_id


def format_scenario_result(name, produce_result, consume_result):
    return "%s[p=%s,c=%s]" % (
        name,
        "ok" if produce_result.ok else "fail:%s" % produce_result.detail,
        "ok" if consume_result.ok else "fail:%s" % consume_result.detail,
    )


def run_case(args, case_obj):
    marker = "auth-case-%s-%s" % (case_obj.topic, uuid.uuid4().hex)
    all_results = []
    details = {}

    for name, use_group, use_auth in SCENARIOS:
        scenario_conf, group_id = build_scenario_conf(
            args, case_obj, use_group=use_group, use_auth=use_auth
        )

        produce_conf = dict(scenario_conf)
        produce_conf.pop("group.id", None)
        produce_conf.pop("auto.offset.reset", None)
        produce_conf.pop("enable.auto.commit", None)
        produce_conf["message.timeout.ms"] = args.message_timeout_ms

        if args.dry_run:
            produce_result = OperationResult(True, "dry_run")
            consume_result = OperationResult(True, "dry_run")
        else:
            produce_result = run_produce(args, case_obj, produce_conf, marker)
            if produce_result.ok:
                consume_marker = "consume-marker-%s-%s" % (
                    case_obj.topic,
                    uuid.uuid4().hex,
                )
                consume_result = run_consume(
                    args, case_obj, scenario_conf, produce_conf, consume_marker
                )
            else:
                consume_result = OperationResult(False, "skipped_produce_failed")

        all_results.append(format_scenario_result(name, produce_result, consume_result))
        details[name] = {
            "group_id": group_id,
            "authenticated": use_auth,
            "bootstrap.servers": scenario_conf.get("bootstrap.servers", ""),
            "security.protocol": scenario_conf.get("security.protocol", ""),
            "produce": dataclasses.asdict(produce_result),
            "consume": dataclasses.asdict(consume_result),
        }

    return " | ".join(all_results), details


def resolve_output_path(args, csv_path):
    if args.in_place:
        return csv_path
    base, ext = os.path.splitext(csv_path)
    return "%s.python_automated%s" % (base, ext or ".csv")


def dump_json(path, payload):
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2, sort_keys=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run auth test cases from CSV and update python SDK result column"
    )
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Input CSV path")
    parser.add_argument(
        "--bootstrap",
        default=DEFAULT_BOOTSTRAP,
        help="Bootstrap servers for tests",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Write results back to the same CSV file",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and render planned scenarios without network calls",
    )
    parser.add_argument(
        "--security-protocol",
        default="SASL_PLAINTEXT",
        help="security.protocol used by authenticated scenarios",
    )
    parser.add_argument(
        "--anonymous-bootstrap",
        default=DEFAULT_ANONYMOUS_BOOTSTRAP,
        help="bootstrap.servers used by anonymous scenarios",
    )
    parser.add_argument(
        "--sasl-mechanisms",
        default="PLAIN",
        help="sasl.mechanisms used by authenticated scenarios",
    )
    parser.add_argument(
        "--anonymous-security-protocol",
        default="",
        help="Optional security.protocol for anonymous scenarios",
    )
    parser.add_argument(
        "--socket-timeout-ms",
        type=int,
        default=4000,
        help="socket.timeout.ms for clients",
    )
    parser.add_argument(
        "--message-timeout-ms",
        type=int,
        default=5000,
        help="message.timeout.ms for producer",
    )
    parser.add_argument(
        "--flush-timeout-sec",
        type=float,
        default=8.0,
        help="Producer flush timeout seconds",
    )
    parser.add_argument(
        "--consume-timeout-sec",
        type=float,
        default=8.0,
        help="Total consume timeout seconds per scenario",
    )
    parser.add_argument(
        "--poll-timeout-sec",
        type=float,
        default=0.5,
        help="Consumer poll timeout seconds",
    )
    parser.add_argument(
        "--assignment-timeout-sec",
        type=float,
        default=10.0,
        help="Max seconds to wait for consumer assignment before probe produce",
    )
    parser.add_argument(
        "--details-json",
        default=".ignore/auth_tests_python_details.json",
        help="Path to dump execution details JSON",
    )
    return parser.parse_args()


def ensure_result_column(fieldnames):
    if FIELD_PYTHON_EFFECT in fieldnames:
        return list(fieldnames)
    return list(fieldnames) + [FIELD_PYTHON_EFFECT]


def run():
    args = parse_args()
    cases, fieldnames, encoding = load_auth_cases(args.csv)
    output_rows = []
    details = {
        "csv": os.path.abspath(args.csv),
        "bootstrap": args.bootstrap,
        "dry_run": args.dry_run,
        "cases": {},
        "errors": [],
    }

    for case_obj in cases:
        try:
            summary, case_detail = run_case(args, case_obj)
            row = dict(case_obj.raw_row)
            row[FIELD_PYTHON_EFFECT] = summary
            output_rows.append(row)
            details["cases"][case_obj.topic] = case_detail
            print("%s -> %s" % (case_obj.topic, summary))
        except Exception as exc:
            row = dict(case_obj.raw_row)
            row[FIELD_PYTHON_EFFECT] = "runner_error:%s" % short_error(exc)
            output_rows.append(row)
            details["errors"].append(
                {
                    "topic": case_obj.topic,
                    "error": short_error(exc),
                    "traceback": traceback.format_exc(),
                }
            )
            print("%s -> runner_error:%s" % (case_obj.topic, short_error(exc)))

    out_path = resolve_output_path(args, args.csv)
    out_fields = ensure_result_column(fieldnames)
    with open(out_path, "w", encoding=encoding, newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(output_rows)

    details["output_csv"] = os.path.abspath(out_path)
    details["generated_at"] = int(time.time())
    dump_json(args.details_json, details)

    print("output_csv=%s" % out_path)
    print("details_json=%s" % args.details_json)


if __name__ == "__main__":
    run()
