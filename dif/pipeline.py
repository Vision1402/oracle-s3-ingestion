"""The load of ONE table, from start to finish.

  1. Work out WHAT to read  (full table, or rows changed since the watermark)
  2. Count + sum in Oracle   (the "expected" numbers)
  3. Extract in chunks -> Parquet -> S3
  4. Validate               (rows written == Oracle count, sums match)
  5. Commit or roll back    (success: advance watermark / replace old snapshot
                             failure: delete this run's files, keep the old state)
  6. Write an audit record  (always, success or failure)
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from decimal import Decimal

import pyarrow as pa

from .config import EnvConfig, TableConfig
from .s3_target import S3Target
from .typemap import columns_from_description, rows_to_table

log = logging.getLogger(__name__)

EPOCH = dt.datetime(1900, 1, 1)
UPPER_BOUND_SQL = "SELECT CAST(SYSTIMESTAMP AS TIMESTAMP) - NUMTODSINTERVAL(:lag, 'MINUTE') FROM dual"


class ValidationError(Exception):
    pass


def _fmt(ts: dt.datetime) -> str:
    return ts.strftime("%Y%m%dT%H%M%S%f")


def plan(cfg: TableConfig, source, target: S3Target) -> dict:
    """Decide the WHERE clause, bind values and batch id for this run."""
    if cfg.load_type == "full":
        return {"where": "", "binds": {}, "wm_from": None, "wm_to": None}

    state = target.read_watermark(cfg.table_id)
    wm_from = dt.datetime.fromisoformat(state["last_watermark"]) if state else EPOCH
    wm_to = source.scalar(UPPER_BOUND_SQL, {"lag": cfg.lag_minutes})
    where = f"WHERE {cfg.watermark_column} > :wm_from AND {cfg.watermark_column} <= :wm_to"
    return {"where": where, "binds": {"wm_from": wm_from, "wm_to": wm_to}, "wm_from": wm_from, "wm_to": wm_to}


def run_table(cfg: TableConfig, env: EnvConfig, source, target: S3Target,
              load_date: str, run_id: str, dry_run: bool = False) -> dict:
    started = time.time()
    audit = {
        "table_id": cfg.table_id, "source": cfg.full_name, "load_type": cfg.load_type,
        "env": env.name, "load_date": load_date, "run_id": run_id,
        "target": f"s3://{env.bucket}/{cfg.target_prefix}/load_date={load_date}/",
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "status": "RUNNING", "rows_written": 0, "files": [], "bytes": 0, "sums": {},
    }
    written_batch_prefix = None

    try:
        p = plan(cfg, source, target)
        audit["watermark_from"] = p["wm_from"].isoformat() if p["wm_from"] else None
        audit["watermark_to"] = p["wm_to"].isoformat() if p["wm_to"] else None

        if cfg.load_type == "full":
            batch_id = f"full_{run_id}"
        else:
            batch_id = f"inc_{_fmt(p['wm_from'])}_{_fmt(p['wm_to'])}"
            if p["wm_to"] <= p["wm_from"]:
                log.info("Nothing new: upper bound %s is not after watermark %s", p["wm_to"], p["wm_from"])
        audit["batch_id"] = batch_id

        # ---- 2. expected numbers from Oracle (same snapshot as the extract) ----
        frm = f"FROM {cfg.full_name} {p['where']}".strip()
        expected_count = int(source.scalar(f"SELECT COUNT(*) {frm}", p["binds"]) or 0)
        audit["source_count"] = expected_count
        expected_sums = {c: Decimal(source.scalar(f"SELECT SUM({c}) {frm}", p["binds"]) or 0)
                         for c in cfg.sum_columns}
        log.info("%s: %d rows to move (%s load, batch %s)", cfg.table_id, expected_count, cfg.load_type, batch_id)

        partition = S3Target.partition_prefix(cfg.target_prefix, load_date)
        batch_prefix = f"{partition}{batch_id}-"
        if dry_run:
            log.info("DRY RUN: would run  SELECT * %s", frm)
            log.info("DRY RUN: would write s3://%s/%spart-NNNNN.parquet", env.bucket, batch_prefix)
            audit["status"] = "DRY_RUN"
            return audit

        # Leftovers from a crashed run of the same incremental window would cause duplicates.
        if cfg.load_type == "incremental":
            leftovers = target.delete_prefix(batch_prefix)
            if leftovers:
                log.warning("Removed %d leftover file(s) from an earlier failed run of this window", leftovers)

        # ---- 3. extract -> parquet -> s3 ----
        written_batch_prefix = batch_prefix
        ingested_at = dt.datetime.now(dt.timezone.utc)
        extra = {"_batch_id": (pa.string(), batch_id),
                 "_ingested_at": (pa.timestamp("us", tz="UTC"), ingested_at)}
        actual_sums = {c: Decimal(0) for c in cfg.sum_columns}
        part = 0
        for description, rows in source.extract(f"SELECT * {frm}", p["binds"], cfg.chunk_size):
            columns = columns_from_description(description)
            names = [c.name for c in columns]
            for c in cfg.sum_columns:
                if c not in names:
                    raise ValidationError(f"sum column {c} is not a column of {cfg.full_name}")
                idx = names.index(c)
                actual_sums[c] += sum((Decimal(r[idx]) for r in rows if r[idx] is not None), Decimal(0))
            table = rows_to_table(columns, rows, extra)
            key = f"{batch_prefix}part-{part:05d}.parquet"
            audit["bytes"] += target.write_parquet(table, key)
            audit["files"].append(key)
            audit["rows_written"] += table.num_rows
            part += 1

        # ---- 4. validate ----
        problems = []
        if audit["rows_written"] != expected_count:
            problems.append(f"row count: Oracle {expected_count} vs written {audit['rows_written']}")
        for c in cfg.sum_columns:
            match = actual_sums[c] == expected_sums[c]
            audit["sums"][c] = {"source": str(expected_sums[c]), "target": str(actual_sums[c]), "match": match}
            if not match:
                problems.append(f"SUM({c}): Oracle {expected_sums[c]} vs written {actual_sums[c]}")
        if problems:
            raise ValidationError("; ".join(problems))

        # ---- 5. commit ----
        if cfg.load_type == "full":
            removed = 0
            paginator = target.s3.get_paginator("list_objects_v2")
            old = [o["Key"] for page in paginator.paginate(Bucket=env.bucket, Prefix=partition)
                   for o in page.get("Contents", []) if not o["Key"].startswith(batch_prefix)]
            for k in old:
                target.s3.delete_object(Bucket=env.bucket, Key=k)
                removed += 1
            if removed:
                log.info("Replaced %d file(s) from an earlier snapshot of %s", removed, load_date)
        else:
            target.write_watermark(cfg.table_id, {
                "last_watermark": max(p["wm_from"], p["wm_to"]).isoformat(),   # never move backwards
                "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "run_id": run_id,
            })
        audit["status"] = "SUCCESS"
        log.info("%s: SUCCESS, %d rows in %d file(s)", cfg.table_id, audit["rows_written"], len(audit["files"]))

    except Exception as exc:  # noqa: BLE001 - every failure must be audited
        audit["status"] = "FAILED"
        audit["error"] = f"{type(exc).__name__}: {exc}"
        log.error("%s: FAILED: %s", cfg.table_id, audit["error"])
        if written_batch_prefix:
            n = target.delete_prefix(written_batch_prefix)
            log.error("Rolled back: deleted %d file(s) written by this run; watermark unchanged", n)
            audit["files"] = []

    finally:
        audit["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        audit["duration_s"] = round(time.time() - started, 2)
        if audit["status"] != "DRY_RUN":
            try:
                key = target.write_audit(cfg.table_id, load_date, run_id, audit)
                log.info("Audit record: s3://%s/%s", env.bucket, key)
            except Exception as exc:  # noqa: BLE001
                log.error("Could not write audit record: %s", exc)

    return audit
