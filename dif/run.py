"""Command-line entry point.

Examples (run from the project folder on the edge node):
  python3 -m dif.run --env dev --list
  python3 -m dif.run --env dev --table northwind_customers --dry-run
  python3 -m dif.run --env dev --table northwind_customers
  python3 -m dif.run --env dev --all
  python3 -m dif.run --env dev --table northwind_orders --reset-watermark

Exit codes (schedulers such as Autosys, Control-M or cron read these):
  0 = success, 1 = at least one table failed, 2 = configuration / usage error
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import secrets
import sys
from pathlib import Path

from .config import ConfigError, load_all_tables, load_env
from .s3_target import S3Target

log = logging.getLogger("dif")


def setup_logging(log_dir: Path, run_id: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"dif_{run_id}.log"
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for old in list(root.handlers):          # avoid duplicate lines if main() runs twice
        root.removeHandler(old)
        old.close()
    for handler in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(fmt)
        root.addHandler(handler)
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return log_file


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="dif", description="Oracle -> S3 raw-layer ingestion")
    ap.add_argument("--env", required=True, help="environment config name, e.g. dev")
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--table", help="table_id to load (file name in config/tables/)")
    what.add_argument("--all", action="store_true", help="load every enabled table")
    what.add_argument("--list", action="store_true", help="list configured tables and exit")
    ap.add_argument("--load-date", help="partition date YYYY-MM-DD (default: today, UTC)")
    ap.add_argument("--dry-run", action="store_true", help="count rows and show the plan, write nothing")
    ap.add_argument("--reset-watermark", action="store_true", help="forget the watermark (next run reloads from scratch)")
    ap.add_argument("--config-dir", default="config")
    ap.add_argument("--log-dir", default="logs")
    args = ap.parse_args(argv)

    now = dt.datetime.now(dt.timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ") + "_" + secrets.token_hex(2)
    log_file = setup_logging(Path(args.log_dir), run_id)

    try:
        load_date = args.load_date or now.strftime("%Y-%m-%d")
        dt.date.fromisoformat(load_date)
        config_dir = Path(args.config_dir)
        env = load_env(config_dir, args.env)
        tables = load_all_tables(config_dir)
    except (ConfigError, ValueError) as exc:
        log.error("Configuration error: %s", exc)
        return 2

    if args.list:
        print(f"{'TABLE_ID':32} {'SOURCE':28} {'LOAD':12} {'ENABLED':8} TARGET")
        for t in tables:
            print(f"{t.table_id:32} {t.full_name:28} {t.load_type:12} {str(t.enabled):8} {t.target_prefix}")
        return 0

    if args.table:
        selected = [t for t in tables if t.table_id == args.table]
        if not selected:
            log.error("No config for table_id '%s'. Try --list.", args.table)
            return 2
    else:
        selected = [t for t in tables if t.enabled]
        skipped = [t.table_id for t in tables if not t.enabled]
        if skipped:
            log.info("Skipping disabled tables: %s", ", ".join(skipped))

    target = S3Target(env.bucket, env.region, env.control_prefix)

    if args.reset_watermark:
        if not args.table:
            log.error("--reset-watermark needs --table")
            return 2
        target.delete_watermark(args.table)
        log.info("Watermark for %s removed; the next run reloads all rows", args.table)
        return 0

    from .oracle_source import OracleSource   # imported here so --list works without Oracle drivers
    from .pipeline import run_table

    log.info("DIF run %s | env=%s | load_date=%s | tables=%d | log=%s",
             run_id, env.name, load_date, len(selected), log_file)
    results = []
    for cfg in selected:
        log.info("-" * 70)
        try:
            with OracleSource(env.oracle_dsn, env.oracle_user_env, env.oracle_password_env) as source:
                results.append(run_table(cfg, env, source, target, load_date, run_id, args.dry_run))
        except Exception as exc:  # noqa: BLE001 - e.g. cannot connect; keep going with other tables
            log.error("%s: FAILED before loading: %s", cfg.table_id, exc)
            results.append({"table_id": cfg.table_id, "status": "FAILED", "rows_written": 0, "error": str(exc)})

    log.info("=" * 70)
    log.info("%-32s %-8s %10s", "TABLE", "STATUS", "ROWS")
    for r in results:
        log.info("%-32s %-8s %10s", r["table_id"], r["status"], r.get("rows_written", 0))
    failed = [r for r in results if r["status"] == "FAILED"]
    log.info("%d succeeded, %d failed", len(results) - len(failed), len(failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
