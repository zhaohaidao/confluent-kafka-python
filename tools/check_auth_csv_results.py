#!/usr/bin/env python3
import argparse
import csv
import re
import sys


DEFAULT_CSV = "auth_tests_verification.python_automated.csv"

FIELD_TOPIC = "topic"
FIELD_EXPECTED = "\u9884\u671f\u6548\u679c"
FIELD_PYTHON_EFFECT = "python SDK \u6548\u679c"

WRITE_PREFIX = "\u5199\uff1a"
READ_PREFIX = "\u8bfb\uff1a"
REAL_USER = "\u5b9e\u540d"
ANON_USER = "\u533f\u540d"
SUCCESS = "\u6210\u529f"
FAILURE = "\u5931\u8d25"

SCENARIOS = (
    "no_group_anonymous",
    "no_group_authenticated",
    "with_group_anonymous",
    "with_group_authenticated",
)


def load_rows(csv_path):
    last_error = None
    for encoding in ("gb2312", "utf-8-sig", "utf-8"):
        try:
            with open(csv_path, "r", encoding=encoding, newline="") as fp:
                return list(csv.DictReader(fp)), encoding
        except UnicodeDecodeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RuntimeError("failed to read csv")


def _expect_ok(mapping, label, source_text):
    if label not in mapping:
        raise ValueError("missing expected label %s in %s" % (label, source_text))
    value = mapping[label]
    if value == SUCCESS:
        return True
    if value == FAILURE:
        return False
    raise ValueError("unexpected expected value %s for %s" % (value, label))


def parse_expected(text):
    if not text:
        raise ValueError("expected result text is empty")
    if READ_PREFIX not in text:
        raise ValueError("missing read section in expected result: %s" % text)

    write_part, read_part = text.split(READ_PREFIX, 1)
    write_part = write_part.split(WRITE_PREFIX, 1)[-1]
    write_results = dict(
        re.findall(r"(%s|%s)\uff1a(%s|%s)" % (REAL_USER, ANON_USER, SUCCESS, FAILURE), write_part)
    )
    read_results = dict(
        re.findall(
            r"(\u4e0d\u5e26group\+%s|\u4e0d\u5e26group\+%s|\u5e26group\+%s|\u5e26group\+%s)\uff1a(%s|%s)"
            % (REAL_USER, ANON_USER, REAL_USER, ANON_USER, SUCCESS, FAILURE),
            read_part,
        )
    )

    return {
        ("no_group_anonymous", "p"): _expect_ok(write_results, ANON_USER, text),
        ("no_group_anonymous", "c"): _expect_ok(read_results, "\u4e0d\u5e26group+" + ANON_USER, text),
        ("no_group_authenticated", "p"): _expect_ok(write_results, REAL_USER, text),
        ("no_group_authenticated", "c"): _expect_ok(read_results, "\u4e0d\u5e26group+" + REAL_USER, text),
        ("with_group_anonymous", "p"): _expect_ok(write_results, ANON_USER, text),
        ("with_group_anonymous", "c"): _expect_ok(read_results, "\u5e26group+" + ANON_USER, text),
        ("with_group_authenticated", "p"): _expect_ok(write_results, REAL_USER, text),
        ("with_group_authenticated", "c"): _expect_ok(read_results, "\u5e26group+" + REAL_USER, text),
    }


def parse_actual(text):
    actual = {}
    for part in (text or "").split(" | "):
        if "[" not in part:
            continue
        name, rest = part.split("[", 1)
        name = name.strip()
        if name not in SCENARIOS:
            continue
        if rest.endswith("]"):
            rest = rest[:-1]
        if not rest.startswith("p=") or ",c=" not in rest:
            continue
        produce_text, consume_text = rest[2:].split(",c=", 1)
        actual[name] = (produce_text.startswith("ok"), consume_text.startswith("ok"))
    return actual


def check_rows(rows, expected_column=FIELD_EXPECTED, actual_column=FIELD_PYTHON_EFFECT):
    failures = []

    for row_index, row in enumerate(rows, 1):
        topic = row.get(FIELD_TOPIC, "<unknown>")
        try:
            expected = parse_expected(row.get(expected_column, ""))
            actual = parse_actual(row.get(actual_column, ""))
        except Exception as exc:
            failures.append(
                {
                    "row": row_index,
                    "topic": topic,
                    "scenario": "row",
                    "detail": "parse_error:%s" % exc,
                }
            )
            continue

        for scenario in SCENARIOS:
            if scenario not in actual:
                failures.append(
                    {
                        "row": row_index,
                        "topic": topic,
                        "scenario": scenario,
                        "detail": "missing_actual",
                    }
                )
                continue

            got_produce, got_consume = actual[scenario]
            for op_name, got in (("p", got_produce), ("c", got_consume)):
                want = expected[(scenario, op_name)]
                if got == want:
                    continue
                failures.append(
                    {
                        "row": row_index,
                        "topic": topic,
                        "scenario": "%s.%s" % (scenario, op_name),
                        "detail": "expected_%s_got_%s"
                        % ("ok" if want else "fail", "ok" if got else "fail"),
                    }
                )

    return failures


def parse_args():
    parser = argparse.ArgumentParser(
        description="Check auth CSV runner output against the expected result column"
    )
    parser.add_argument("csv", nargs="?", default=DEFAULT_CSV, help="CSV generated by run_auth_csv_tests.py")
    parser.add_argument(
        "--expected-column",
        default=FIELD_EXPECTED,
        help="Column containing the expected auth matrix",
    )
    parser.add_argument(
        "--actual-column",
        default=FIELD_PYTHON_EFFECT,
        help="Column containing the Python SDK runner result",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rows, encoding = load_rows(args.csv)
    failures = check_rows(
        rows,
        expected_column=args.expected_column,
        actual_column=args.actual_column,
    )

    print(
        "auth_e2e_cases=%d failures=%d encoding=%s csv=%s"
        % (len(rows), len(failures), encoding, args.csv)
    )
    for failure in failures:
        print(
            "AUTH_E2E_FAIL row=%s topic=%s scenario=%s %s"
            % (
                failure["row"],
                failure["topic"],
                failure["scenario"],
                failure["detail"],
            )
        )

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
