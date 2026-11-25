#!/usr/bin/env python3
"""Summarize live attention and exported runtime logs."""

import argparse
import collections
import datetime
import http.cookiejar
import json
import math
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
TIMESTAMP = re.compile(r"^(\d{4}-\d\d-\d\dT\S+)")
ERROR = re.compile(r"(?:^|\s)ERROR(?:\s|$)")
LOCKED = re.compile(
    r"database (?:table |schema )?is (?:locked|busy)|sqlite_(?:busy|locked)",
    re.IGNORECASE,
)
ELAPSED = re.compile(r"\belapsed_secs=(\S+)")
STATES = {"ready", "running", "waiting", "blocked", "failed", "stalled", "unavailable"}
COMPLETENESS = {"complete", "partial", "unavailable"}
ATTENTION_PROBLEMS = {"waiting", "blocked", "failed", "stalled", "unavailable"}
MAX_REPORTED_GROUPS = 20
MAX_REPORTED_EXAMPLES = 5


def parse_timestamp(value):
    try:
        parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else None


def summarize_logs(lines):
    result = {
        "records": 0,
        "errors": 0,
        "locked_busy": 0,
        "slow_over_one_second": 0,
        "unclassified_slow": 0,
        "untimestamped": 0,
        "first": None,
        "last": None,
    }
    first = last = None
    for raw in lines:
        line = ANSI.sub("", raw).strip()
        if not line or line in ("=== stdout ===", "=== stderr ==="):
            continue
        result["records"] += 1
        match = TIMESTAMP.match(line)
        timestamp = parse_timestamp(match.group(1)) if match else None
        if timestamp is None:
            result["untimestamped"] += 1
        else:
            if first is None or timestamp < first:
                first, result["first"] = timestamp, match.group(1)
            if last is None or timestamp > last:
                last, result["last"] = timestamp, match.group(1)
        result["errors"] += bool(ERROR.search(line))
        result["locked_busy"] += bool(LOCKED.search(line))
        if "slow statement:" in line:
            elapsed = ELAPSED.search(line)
            try:
                seconds = float(elapsed.group(1)) if elapsed else float("nan")
            except ValueError:
                seconds = float("nan")
            if not math.isfinite(seconds) or seconds < 0:
                result["unclassified_slow"] += 1
            elif seconds > 1:
                result["slow_over_one_second"] += 1
    return result


def summarize_attention(snapshot):
    if not isinstance(snapshot, dict):
        raise ValueError("attention snapshot is not an object")
    observed_at = snapshot.get("observed_at")
    completeness = snapshot.get("completeness")
    items = snapshot.get("items")
    if (
        not isinstance(observed_at, str)
        or parse_timestamp(observed_at) is None
        or not isinstance(completeness, str)
        or completeness not in COMPLETENESS
        or not isinstance(items, list)
    ):
        raise ValueError("attention snapshot is missing valid observation data")
    by_state = collections.Counter()
    blocking_groups = 0
    problems = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("attention item is not an object")
        state = item.get("state")
        blocks_work = item.get("blocks_work")
        identifier = item.get("id")
        examples = item.get("examples")
        title, message, count, next_at = (
            item.get("title"),
            item.get("message"),
            item.get("count"),
            item.get("next_at"),
        )
        if (
            not isinstance(state, str)
            or state not in STATES
            or not isinstance(blocks_work, bool)
            or not isinstance(identifier, str)
            or not identifier.strip()
            or not isinstance(examples, list)
            or not isinstance(title, str)
            or not isinstance(message, str)
            or not isinstance(count, int)
            or isinstance(count, bool)
            or count < 0
            or (
                next_at is not None
                and (not isinstance(next_at, str) or parse_timestamp(next_at) is None)
            )
        ):
            raise ValueError("attention item has invalid details")
        for example in examples:
            if (
                not isinstance(example, dict)
                or not isinstance(example.get("resource"), str)
                or (
                    example.get("detail") is not None
                    and not isinstance(example["detail"], str)
                )
            ):
                raise ValueError("attention example has invalid details")
        by_state[state] += 1
        blocking_groups += blocks_work
        if state in ATTENTION_PROBLEMS or blocks_work:
            problems.append((identifier, state, title, message, count, next_at, examples))
    return {
        "observed_at": observed_at,
        "completeness": completeness,
        "by_state": by_state,
        "blocking_groups": blocking_groups,
        "problems": problems,
    }


def base_url(value):
    parsed = urllib.parse.urlsplit(value)
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("instance URL must be an HTTP(S) URL without credentials or a query")
    return value.rstrip("/")


def request(opener, url, *, data=None, headers=None, method="GET"):
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with opener.open(req, timeout=15) as response:
            content = response.read(4 * 1024 * 1024 + 1)
            if len(content) > 4 * 1024 * 1024:
                raise ValueError("response exceeds 4 MiB")
            return content
    except urllib.error.HTTPError as error:
        status = error.code
        error.close()
        raise ValueError(f"HTTP {status}") from error
    except urllib.error.URLError as error:
        raise ValueError(f"connection failed: {error.reason}") from error


def fetch_health(url):
    body = request(urllib.request.build_opener(), f"{url}/healthz")
    if body.strip() != b"ok":
        raise ValueError("health endpoint did not return ok")


def fetch_attention(url, user_env, password_env):
    username = os.environ.get(user_env)
    password = os.environ.get(password_env)
    if not username or not password:
        raise ValueError(f"credentials are missing from {user_env} or {password_env}")
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
    )
    headers = {"Content-Type": "application/json", "x-ism-request": "1"}
    request(
        opener,
        f"{url}/api/admin/login",
        data=json.dumps({"username": username, "password": password}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        snapshot = json.loads(request(opener, f"{url}/api/admin/attention"))
    except (ValueError, OSError, TimeoutError) as error:
        try:
            request(
                opener, f"{url}/api/admin/logout", headers={"x-ism-request": "1"}, method="POST"
            )
        except (ValueError, OSError, TimeoutError) as logout_error:
            raise ValueError(f"{error}; logout failed: {logout_error}") from error
        raise
    try:
        request(opener, f"{url}/api/admin/logout", headers={"x-ism-request": "1"}, method="POST")
    except (ValueError, OSError, TimeoutError) as error:
        raise ValueError(f"logout failed: {error}") from error
    return snapshot


def named_options(values, option):
    result = {}
    for name, *arguments in values or []:
        if name in result:
            raise ValueError(f"{option} specified more than once for {name}")
        result[name] = arguments
    return result


def print_attention(summary):
    states = summary["by_state"]
    print(
        f"  attention: {summary['completeness']} at {summary['observed_at']}; "
        f"waiting={states['waiting']}, blocked={states['blocked']}, "
        f"stalled={states['stalled']}, failed={states['failed']}, "
        f"unavailable={states['unavailable']}; "
        f"blocking groups={summary['blocking_groups']}"
    )
    print("  Attention groups can overlap; these are not distinct item totals.")
    def compact(value):
        return re.sub(r"\s+", " ", ANSI.sub("", value)).strip()

    for identifier, state, title, message, count, next_at, examples in summary["problems"][:MAX_REPORTED_GROUPS]:
        print(f"    {state}: {compact(title)} ({count}) [{compact(identifier)}] - {compact(message)}")
        if next_at:
            print(f"      next reported time: {next_at}")
        for example in examples[:MAX_REPORTED_EXAMPLES]:
            detail = example.get("detail")
            suffix = f" - {compact(detail)}" if detail else ""
            print(f"      example: {compact(example['resource'])}{suffix}")
        if len(examples) > MAX_REPORTED_EXAMPLES:
            print(f"      {len(examples) - MAX_REPORTED_EXAMPLES} more examples omitted.")
    omitted = len(summary["problems"]) - MAX_REPORTED_GROUPS
    if omitted > 0:
        print(f"    {omitted} more attention groups omitted.")


def print_logs(summary):
    if not summary["records"]:
        print("  logs: no records; coverage unavailable")
        return
    print(
        f"  logs: errors={summary['errors']}, locked/busy={summary['locked_busy']}, "
        f"queries over 1s={summary['slow_over_one_second']}, "
        f"unclassified slow queries={summary['unclassified_slow']}, "
        f"records={summary['records']}"
    )
    print(
        f"  log entries: {summary['first'] or 'unknown'} to {summary['last'] or 'unknown'}; "
        f"untimestamped={summary['untimestamped']} (retention coverage not established)"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--instance", nargs=3, action="append", required=True, metavar=("NAME", "URL", "LOG")
    )
    parser.add_argument(
        "--credentials",
        nargs=3,
        action="append",
        metavar=("NAME", "USER_ENV", "PASSWORD_ENV"),
        help="environment variable names for an admin login",
    )
    parser.add_argument(
        "--snapshot",
        nargs=2,
        action="append",
        metavar=("NAME", "FILE"),
        help="use a previously exported admin attention snapshot instead of logging in",
    )
    args = parser.parse_args(argv)
    try:
        instances = named_options(args.instance, "--instance")
        credentials = named_options(args.credentials, "--credentials")
        snapshots = named_options(args.snapshot, "--snapshot")
        if (credentials.keys() | snapshots.keys()) - instances.keys():
            raise ValueError("credentials or snapshot specified for an unknown instance")
        if credentials.keys() & snapshots.keys():
            raise ValueError("choose credentials or a snapshot for each instance, not both")
    except ValueError as error:
        parser.error(str(error))

    failed = False
    for name, (raw_url, log_path) in instances.items():
        print(f"{name}:")
        try:
            url = base_url(raw_url)
            fetch_health(url)
            print("  health: ok")
        except (ValueError, OSError, TimeoutError) as error:
            print(f"  health: unavailable ({error})")
            url = None
            failed = True
        try:
            if name in snapshots:
                with open(snapshots[name][0], encoding="utf-8") as file:
                    snapshot = json.load(file)
            elif name in credentials and url is not None:
                snapshot = fetch_attention(url, *credentials[name])
            else:
                raise ValueError("no usable credentials or exported attention snapshot")
            summary = summarize_attention(snapshot)
            print_attention(summary)
            failed |= summary["completeness"] != "complete"
            failed |= summary["blocking_groups"] > 0 or any(
                summary["by_state"][state] > 0
                for state in ("blocked", "failed", "stalled", "unavailable")
            )
        except (ValueError, OSError, TimeoutError) as error:
            print(f"  attention: unavailable ({error})")
            failed = True
        try:
            with open(log_path, encoding="utf-8") as file:
                summary = summarize_logs(file)
            print_logs(summary)
            failed |= (
                not summary["records"]
                or summary["errors"] != 0
                or summary["locked_busy"] != 0
                or summary["untimestamped"] != 0
                or summary["unclassified_slow"] != 0
            )
        except (ValueError, OSError) as error:
            print(f"  logs: unavailable ({error})")
            failed = True
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
