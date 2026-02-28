#!/usr/bin/env python3
import argparse
import csv
import dataclasses
import glob
import json
import os
import subprocess
import sys
import time
import traceback
import uuid

DEFAULT_BOOTSTRAP = "10.142.247.201:9093,10.142.247.204:9093,10.142.247.205:9093"
DEFAULT_ANONYMOUS_BOOTSTRAP = "10.142.247.201:9092,10.142.247.204:9092,10.142.247.205:9092"
DEFAULT_CSV = "auth_tests_verification.csv"
DEFAULT_JAVA_COL = "java SDK 效果（新版本）"
GRADLE_CACHE = "/root/.gradle/caches/modules-2/files-2.1"

REQUIRED_RUNTIME_JAR_GLOBS = (
    GRADLE_CACHE + "/com.github.luben/zstd-jni/1.4.4-7/*/zstd-jni-1.4.4-7.jar",
    GRADLE_CACHE + "/org.lz4/lz4-java/1.7.1/*/lz4-java-1.7.1.jar",
    GRADLE_CACHE + "/org.xerial.snappy/snappy-java/1.1.7.3/*/snappy-java-1.1.7.3.jar",
    GRADLE_CACHE + "/org.slf4j/slf4j-api/1.7.30/*/slf4j-api-1.7.30.jar",
    GRADLE_CACHE + "/com.fasterxml.jackson.core/jackson-databind/2.10.5.1/*/jackson-databind-2.10.5.1.jar",
    GRADLE_CACHE + "/com.fasterxml.jackson.core/jackson-annotations/2.10.5/*/jackson-annotations-2.10.5.jar",
    GRADLE_CACHE + "/com.fasterxml.jackson.core/jackson-core/2.10.5/*/jackson-core-2.10.5.jar",
    GRADLE_CACHE + "/com.xiaohongshu.data/swim-lane-utils/1.1.7-RELEASE/*/swim-lane-utils-1.1.7-RELEASE.jar",
    GRADLE_CACHE + "/com.xiaohongshu/infra-framework-context/3.3.7-RELEASE/*/infra-framework-context-3.3.7-RELEASE.jar",
    GRADLE_CACHE + "/com.xiaohongshu/rpc-context/3.3.7-RELEASE/*/rpc-context-3.3.7-RELEASE.jar",
    GRADLE_CACHE + "/com.xiaohongshu.infra/rpc-context/1.1.7/*/rpc-context-1.1.7.jar",
    GRADLE_CACHE + "/org.apache.thrift/libthrift/0.11.0/*/libthrift-0.11.0.jar",
    GRADLE_CACHE + "/org.apache.httpcomponents/httpcore/4.4.13/*/httpcore-4.4.13.jar",
    GRADLE_CACHE + "/com.alibaba/transmittable-thread-local/2.2.0/*/transmittable-thread-local-2.2.0.jar",
    GRADLE_CACHE + "/org.apache.commons/commons-lang3/3.12.0/*/commons-lang3-3.12.0.jar",
    GRADLE_CACHE + "/org.apache.httpcomponents/httpclient/4.5.11/*/httpclient-4.5.11.jar",
    GRADLE_CACHE + "/commons-codec/commons-codec/1.11/*/commons-codec-1.11.jar",
    GRADLE_CACHE + "/org.slf4j/jcl-over-slf4j/1.7.25/*/jcl-over-slf4j-1.7.25.jar",
)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)

FIELD_TOPIC = "topic"
FIELD_GROUP = "group"
FIELD_ACCOUNT = "账号密码"

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


def short_error(exc):
    if exc is None:
        return ""
    text = str(exc).replace("\n", " ").strip()
    if len(text) > 200:
        return text[:197] + "..."
    return text


def parse_account_pair(account_pair):
    pair = (account_pair or "").strip()
    if not pair or "=" not in pair:
        raise ValueError("account pair should be username=password")
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


def ensure_result_column(fieldnames, col_name):
    if col_name in fieldnames:
        return list(fieldnames)
    return list(fieldnames) + [col_name]


def resolve_output_path(args, csv_path):
    if args.in_place:
        return csv_path
    base, ext = os.path.splitext(csv_path)
    return "%s.java_automated%s" % (base, ext or ".csv")


def resolve_runtime_jars(args):
    jars = []
    if args.kafka_clients_classes_dir and os.path.isdir(args.kafka_clients_classes_dir):
        jars.append(args.kafka_clients_classes_dir)

    classes_resource_dir = os.path.join(
        os.path.dirname(args.kafka_clients_classes_dir.rstrip("/")),
        "resources",
        "main",
    )
    if os.path.isdir(classes_resource_dir):
        jars.append(classes_resource_dir)

    jars.append(args.kafka_clients_jar)
    missing = []

    for pattern in REQUIRED_RUNTIME_JAR_GLOBS:
        matches = sorted(glob.glob(pattern))
        if not matches:
            missing.append(pattern)
            continue
        jars.append(matches[0])

    if args.extra_classpath:
        for item in args.extra_classpath.split(os.pathsep):
            item = item.strip()
            if item:
                jars.append(item)

    dedup = []
    seen = set()
    for item in jars:
        if item in seen:
            continue
        seen.add(item)
        dedup.append(item)
    return dedup, missing


def compile_java_runner(args, runtime_jars):
    source_file = os.path.join(REPO_ROOT, ".ignore", "java_auth_runner", "src", "AuthScenarioCli.java")
    class_dir = os.path.join(REPO_ROOT, ".ignore", "java_auth_runner", "classes")
    class_file = os.path.join(class_dir, "AuthScenarioCli.class")

    os.makedirs(class_dir, exist_ok=True)

    if not os.path.exists(source_file):
        raise RuntimeError("java source file not found: %s" % source_file)

    need_compile = (
        (not os.path.exists(class_file))
        or os.path.getmtime(class_file) < os.path.getmtime(source_file)
        or args.force_compile
    )
    if not need_compile:
        return class_dir

    classpath = os.pathsep.join(runtime_jars)

    cmd = [
        args.javac,
        "-cp",
        classpath,
        "-d",
        class_dir,
        source_file,
    ]

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "javac failed rc=%s stdout=%s stderr=%s"
            % (proc.returncode, proc.stdout.strip(), proc.stderr.strip())
        )
    return class_dir


def parse_runner_output(text):
    result = {}
    for line in (text or "").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        result[key.strip()] = value.strip()
    return result


def run_java_scenario(args, case_obj, scenario_name, use_group, use_auth, class_dir, runtime_jars):
    bootstrap = args.bootstrap if use_auth else args.anonymous_bootstrap
    group_id = case_obj.group if use_group else "no-group-%s-%s" % (case_obj.topic, uuid.uuid4().hex[:8])

    username = ""
    password = ""
    if use_auth:
        username, password = parse_account_pair(case_obj.account_pair)

    classpath = os.pathsep.join([class_dir] + list(runtime_jars))

    cmd = [
        args.java,
        "-cp",
        classpath,
        "AuthScenarioCli",
        "--bootstrap",
        bootstrap,
        "--topic",
        case_obj.topic,
        "--group",
        group_id,
        "--use-group",
        "true" if use_group else "false",
        "--use-auth",
        "true" if use_auth else "false",
        "--username",
        username,
        "--password",
        password,
        "--security-protocol",
        args.security_protocol,
        "--anonymous-security-protocol",
        args.anonymous_security_protocol,
        "--sasl-mechanism",
        args.sasl_mechanisms,
        "--socket-timeout-ms",
        str(args.socket_timeout_ms),
        "--request-timeout-ms",
        str(args.request_timeout_ms),
        "--assignment-timeout-ms",
        str(args.assignment_timeout_ms),
        "--consume-timeout-ms",
        str(args.consume_timeout_ms),
        "--poll-timeout-ms",
        str(args.poll_timeout_ms),
        "--send-timeout-ms",
        str(args.send_timeout_ms),
        "--client-id-prefix",
        args.client_id_prefix,
    ]

    started = time.time()
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=args.scenario_timeout_sec,
    )
    cost_ms = int((time.time() - started) * 1000)

    parsed = parse_runner_output(proc.stdout)
    produce_ok = parsed.get("produce_ok", "false").lower() == "true"
    consume_ok = parsed.get("consume_ok", "false").lower() == "true"
    produce_detail = parsed.get("produce_detail", "unknown")
    consume_detail = parsed.get("consume_detail", "unknown")
    real_group = parsed.get("group_id", group_id)

    produce_result = OperationResult(produce_ok, produce_detail)
    consume_result = OperationResult(consume_ok, consume_detail)

    detail = {
        "scenario": scenario_name,
        "use_group": use_group,
        "use_auth": use_auth,
        "topic": case_obj.topic,
        "group_id": real_group,
        "bootstrap": bootstrap,
        "command": cmd,
        "return_code": proc.returncode,
        "cost_ms": cost_ms,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "produce": dataclasses.asdict(produce_result),
        "consume": dataclasses.asdict(consume_result),
    }

    if proc.returncode != 0:
        if not produce_result.ok:
            produce_result.detail = "runner_rc_%d:%s" % (
                proc.returncode,
                short_error(proc.stderr or proc.stdout),
            )
        if consume_result.detail == "unknown":
            consume_result = OperationResult(False, "runner_rc_%d" % proc.returncode)

    return produce_result, consume_result, detail


def format_scenario_result(name, produce_result, consume_result):
    return "%s[p=%s,c=%s]" % (
        name,
        "ok" if produce_result.ok else "fail:%s" % produce_result.detail,
        "ok" if consume_result.ok else "fail:%s" % consume_result.detail,
    )


def run_case(args, case_obj, class_dir, runtime_jars):
    parts = []
    detail = {}
    for scenario_name, use_group, use_auth in SCENARIOS:
        produce_result, consume_result, scenario_detail = run_java_scenario(
            args,
            case_obj,
            scenario_name,
            use_group,
            use_auth,
            class_dir,
            runtime_jars,
        )
        parts.append(format_scenario_result(scenario_name, produce_result, consume_result))
        detail[scenario_name] = scenario_detail
    return " | ".join(parts), detail


def dump_json(path, payload):
    with open(path, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2, sort_keys=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run Java auth test cases from CSV and update Java result column"
    )
    parser.add_argument("--csv", default=DEFAULT_CSV)
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--anonymous-bootstrap", default=DEFAULT_ANONYMOUS_BOOTSTRAP)
    parser.add_argument("--in-place", action="store_true")
    parser.add_argument("--details-json", default=".ignore/auth_tests_verification_java_details.json")
    parser.add_argument("--java-column", default=DEFAULT_JAVA_COL)

    parser.add_argument("--security-protocol", default="SASL_PLAINTEXT")
    parser.add_argument("--anonymous-security-protocol", default="")
    parser.add_argument("--sasl-mechanisms", default="SCRAM-SHA-256")

    parser.add_argument("--socket-timeout-ms", type=int, default=4000)
    parser.add_argument("--request-timeout-ms", type=int, default=7000)
    parser.add_argument("--assignment-timeout-ms", type=int, default=10000)
    parser.add_argument("--consume-timeout-ms", type=int, default=12000)
    parser.add_argument("--poll-timeout-ms", type=int, default=500)
    parser.add_argument("--send-timeout-ms", type=int, default=8000)
    parser.add_argument("--scenario-timeout-sec", type=float, default=40.0)
    parser.add_argument("--client-id-prefix", default="java-auth")

    parser.add_argument(
        "--kafka-clients-jar",
        default="/home/admin/mh/kafka/RED-Kafka-2.6.2/clients/build/libs/kafka-clients-2.6.2-r0.1-SNAPSHOT.jar",
    )
    parser.add_argument(
        "--kafka-clients-classes-dir",
        default="/home/admin/mh/kafka/RED-Kafka-2.6.2/clients/build/classes/java/main",
    )
    parser.add_argument("--extra-classpath", default="")
    parser.add_argument("--java", default="java")
    parser.add_argument("--javac", default="javac")
    parser.add_argument("--force-compile", action="store_true")
    return parser.parse_args()


def run():
    args = parse_args()
    cases, fieldnames, encoding = load_auth_cases(args.csv)

    if not os.path.exists(args.kafka_clients_jar):
        raise RuntimeError("kafka-clients jar not found: %s" % args.kafka_clients_jar)

    runtime_jars, missing_runtime = resolve_runtime_jars(args)
    if missing_runtime:
        raise RuntimeError("missing required runtime jars: %s" % ", ".join(missing_runtime))

    class_dir = compile_java_runner(args, runtime_jars)

    details = {
        "csv": os.path.abspath(args.csv),
        "output_csv": "",
        "bootstrap": args.bootstrap,
        "anonymous_bootstrap": args.anonymous_bootstrap,
        "kafka_clients_jar": args.kafka_clients_jar,
        "kafka_clients_classes_dir": args.kafka_clients_classes_dir,
        "runtime_jars": runtime_jars,
        "cases": {},
        "errors": [],
        "generated_at": int(time.time()),
    }

    rows = []
    for case_obj in cases:
        try:
            summary, case_detail = run_case(args, case_obj, class_dir, runtime_jars)
            row = dict(case_obj.raw_row)
            row[args.java_column] = summary
            rows.append(row)
            details["cases"][case_obj.topic] = case_detail
            print("%s -> %s" % (case_obj.topic, summary))
        except Exception as exc:
            row = dict(case_obj.raw_row)
            row[args.java_column] = "runner_error:%s" % short_error(exc)
            rows.append(row)
            details["errors"].append(
                {
                    "topic": case_obj.topic,
                    "error": short_error(exc),
                    "traceback": traceback.format_exc(),
                }
            )
            print("%s -> runner_error:%s" % (case_obj.topic, short_error(exc)))

    out_path = resolve_output_path(args, args.csv)
    out_fields = ensure_result_column(fieldnames, args.java_column)
    with open(out_path, "w", encoding=encoding, newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(rows)

    details["output_csv"] = os.path.abspath(out_path)
    dump_json(args.details_json, details)

    print("output_csv=%s" % out_path)
    print("details_json=%s" % args.details_json)


if __name__ == "__main__":
    run()
