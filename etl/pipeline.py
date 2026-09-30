"""Config-driven ETL pipeline: extract, transform, validate, load, reconcile.

Design notes worth knowing before reading the code:

* The mapping lives in JSON, not in this file. Adding a field or a rule is a
  config change, not a code change, which is how migration tooling has to work
  once there is more than one source system.
* Bad rows are quarantined with a reason code rather than failing the run. A
  migration that stops on the first malformed row is a migration nobody can
  finish; a migration that silently drops rows is worse.
* The load is idempotent. Re-running the pipeline against the same source
  produces the same target state instead of duplicating rows, because in
  practice you will run it more than once.
* Reconciliation is a first-class output, not an afterthought. The question
  "did everything arrive" needs an answer you can hand to someone.
"""

import argparse
import csv
import json
import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path

from etl import transforms
from etl.validate import FieldError, validate_field, validate_record_rules

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load_config(path: Path) -> dict:
    with open(path) as fh:
        return json.load(fh)


def extract(source_path: Path):
    """Yield (line_number, raw_row_dict) from the source file."""
    with open(source_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader, start=2):  # line 1 is the header
            yield i, {k: (v or "").strip() for k, v in row.items() if k is not None}


def transform_and_validate(raw_row: dict, config: dict, today: date = None):
    """Return (record, errors) for one source row."""
    record, errors = {}, []

    for spec in config["fields"]:
        raw_value = raw_row.get(spec["source"], "")
        try:
            cleaned = transforms.apply_all(raw_value, spec.get("transforms"))
            record[spec["target"]] = validate_field(cleaned, spec, today=today)
        except FieldError as err:
            errors.append({"code": err.code, "field": err.field, "value": err.value})

    if not errors:
        try:
            validate_record_rules(record, config.get("record_rules"))
        except FieldError as err:
            errors.append({"code": err.code, "field": err.field, "value": err.value})

    return record, errors


def create_schema(conn: sqlite3.Connection, config: dict) -> None:
    columns = ", ".join(f"{f['target']} TEXT" for f in config["fields"])
    key = config["natural_key"]
    conn.execute(f"CREATE TABLE IF NOT EXISTS {config['target_table']} ({columns}, PRIMARY KEY ({key}))")
    conn.execute(
        """CREATE TABLE IF NOT EXISTS quarantine (
               feed_name TEXT, source_line INTEGER, reason_code TEXT,
               field TEXT, bad_value TEXT, raw_row TEXT)"""
    )
    conn.execute("DELETE FROM quarantine WHERE feed_name = ?", (config["feed_name"],))
    conn.commit()


def load(conn: sqlite3.Connection, config: dict, records) -> int:
    """Upsert records on the natural key so re-runs do not duplicate."""
    targets = [f["target"] for f in config["fields"]]
    placeholders = ", ".join("?" for _ in targets)
    updates = ", ".join(f"{t}=excluded.{t}" for t in targets if t != config["natural_key"])
    sql = (
        f"INSERT INTO {config['target_table']} ({', '.join(targets)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT({config['natural_key']}) DO UPDATE SET {updates}"
    )
    rows = [tuple(_serialize(r.get(t)) for t in targets) for r in records]
    conn.executemany(sql, rows)
    conn.commit()
    return len(rows)


def _serialize(value):
    if value is None:
        return None
    if isinstance(value, (date, Decimal)):
        return str(value)
    return value


def quarantine(conn: sqlite3.Connection, config: dict, rejects) -> None:
    conn.executemany(
        "INSERT INTO quarantine VALUES (?, ?, ?, ?, ?, ?)",
        [
            (config["feed_name"], line, e["code"], e["field"], e["value"], json.dumps(raw))
            for line, raw, errs in rejects
            for e in errs
        ],
    )
    conn.commit()


def reconcile(conn: sqlite3.Connection, config: dict, stats: dict) -> str:
    """Build a reconciliation report comparing source, target, and rejects."""
    table = config["target_table"]
    loaded = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    premium_total = conn.execute(
        f"SELECT COALESCE(SUM(CAST(monthly_premium AS REAL)), 0) FROM {table}"
    ).fetchone()[0]
    by_reason = conn.execute(
        "SELECT reason_code, COUNT(*) FROM quarantine WHERE feed_name = ?"
        " GROUP BY reason_code ORDER BY COUNT(*) DESC",
        (config["feed_name"],),
    ).fetchall()
    by_status = conn.execute(
        f"SELECT status, COUNT(*) FROM {table} GROUP BY status ORDER BY COUNT(*) DESC"
    ).fetchall()

    accounted = stats["accepted"] + stats["rejected"]
    balanced = accounted == stats["source_rows"]

    lines = [
        f"# Reconciliation report: {config['feed_name']}",
        "",
        f"Run date: {date.today().isoformat()}",
        f"Source file: `{config['source_file']}`",
        f"Target table: `{table}`",
        "",
        "## Row counts",
        "",
        "| Measure | Count |",
        "| --- | ---: |",
        f"| Rows read from source | {stats['source_rows']} |",
        f"| Rows accepted | {stats['accepted']} |",
        f"| Rows quarantined | {stats['rejected']} |",
        f"| Rows in target table | {loaded} |",
        "",
        f"**Balance check:** accepted + quarantined = {accounted}, "
        f"source = {stats['source_rows']} "
        f"{'PASS' if balanced else 'FAIL'}",
        "",
        "## Control totals",
        "",
        f"Sum of monthly_premium in target: {premium_total:,.2f}",
        "",
        "## Rejections by reason code",
        "",
        "| Reason code | Count |",
        "| --- | ---: |",
    ]
    lines += [f"| {code} | {count} |" for code, count in by_reason] or ["| (none) | 0 |"]
    lines += ["", "## Loaded rows by status", "", "| Status | Count |", "| --- | ---: |"]
    lines += [f"| {status} | {count} |" for status, count in by_status]
    lines.append("")
    return "\n".join(lines)


def run(config_path: Path, db_path: Path, report_path: Path, today: date = None) -> dict:
    config = load_config(config_path)
    source = PROJECT_ROOT / config["source_file"]

    accepted, rejects, seen_keys = [], [], set()
    source_rows = 0
    key_field = config["natural_key"]

    for line_no, raw in extract(source):
        source_rows += 1
        record, errors = transform_and_validate(raw, config, today=today)

        key = record.get(key_field)
        if not errors and key in seen_keys:
            errors.append({"code": "DUPLICATE_KEY", "field": key_field, "value": str(key)})

        if errors:
            rejects.append((line_no, raw, errors))
        else:
            seen_keys.add(key)
            accepted.append(record)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        create_schema(conn, config)
        load(conn, config, accepted)
        quarantine(conn, config, rejects)
        stats = {
            "source_rows": source_rows,
            "accepted": len(accepted),
            "rejected": len(rejects),
        }
        report = reconcile(conn, config, stats)
    finally:
        conn.close()

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report)
    return stats


def main():
    parser = argparse.ArgumentParser(description="Config-driven healthcare ETL pipeline")
    parser.add_argument("--config", default="config/members_mapping.json")
    parser.add_argument("--db", default="output/warehouse.db")
    parser.add_argument("--report", default="output/reconciliation.md")
    args = parser.parse_args()

    stats = run(
        PROJECT_ROOT / args.config,
        PROJECT_ROOT / args.db,
        PROJECT_ROOT / args.report,
    )
    print(
        f"Read {stats['source_rows']} rows | "
        f"loaded {stats['accepted']} | quarantined {stats['rejected']}"
    )
    print(f"Reconciliation report written to {args.report}")


if __name__ == "__main__":
    main()
