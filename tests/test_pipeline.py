"""End-to-end tests with a fake Oracle source and a mocked S3 (moto).

Run:  python3 -m pytest -q
"""
import datetime as dt
import io
from decimal import Decimal
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from dif.config import ConfigError, EnvConfig, TableConfig, load_table
from dif.pipeline import run_table
from dif.s3_target import S3Target

BUCKET = "test-raw-bucket"


class T:  # fake oracledb DbType
    def __init__(self, name):
        self.name = name


# (name, type_code, display_size, internal_size, precision, scale, null_ok)
DESCRIPTION = [
    ("ORDER_ID", T("DB_TYPE_NUMBER"), None, None, 10, 0, False),
    ("ORDER_DATE", T("DB_TYPE_DATE"), None, None, None, None, False),
    ("NOTES", T("DB_TYPE_CLOB"), None, None, None, None, True),
    ("ORDER_TOTAL", T("DB_TYPE_NUMBER"), None, None, 0, -127, True),
    ("LAST_UPDATED", T("DB_TYPE_DATE"), None, None, None, None, True),
]
WM_IDX = 4


class FakeSource:
    def __init__(self, rows, now, count_offset=0):
        self.rows, self.now, self.count_offset = rows, now, count_offset

    def _filter(self, binds):
        if "wm_from" in binds:
            return [r for r in self.rows if binds["wm_from"] < r[WM_IDX] <= binds["wm_to"]]
        return list(self.rows)

    def scalar(self, sql, binds=None):
        binds = binds or {}
        if "SYSTIMESTAMP" in sql:
            return self.now - dt.timedelta(minutes=binds["lag"])
        rows = self._filter(binds)
        if "COUNT(*)" in sql:
            return len(rows) + self.count_offset
        if "SUM(" in sql:
            col = sql.split("SUM(")[1].split(")")[0]
            idx = [d[0] for d in DESCRIPTION].index(col)
            vals = [r[idx] for r in rows if r[idx] is not None]
            return sum(vals, Decimal(0)) if vals else None
        raise AssertionError(sql)

    def extract(self, sql, binds, chunk_size):
        rows = self._filter(binds)
        for i in range(0, len(rows), chunk_size):
            yield DESCRIPTION, rows[i:i + chunk_size]


def make_rows(n, start_id=1, ts=dt.datetime(2026, 1, 1, 10, 0, 0)):
    out = []
    for i in range(n):
        oid = start_id + i
        out.append((Decimal(oid), ts + dt.timedelta(minutes=oid), 'gift wrap, "fragile"\nring twice' if oid % 5 == 0 else None,
                    Decimal("1234.56789") + oid, ts + dt.timedelta(minutes=oid)))
    return out


ENV = EnvConfig(name="test", region="us-east-1", bucket=BUCKET, oracle_dsn="x",
                oracle_user_env="U", oracle_password_env="P")


def cfg(load_type, **kw):
    return TableConfig(table_id="orders", schema="NORTHWIND", table="ORDERS", load_type=load_type,
                       target_prefix="raw/oracle/northwind/orders",
                       watermark_column="LAST_UPDATED" if load_type == "incremental" else None,
                       chunk_size=kw.pop("chunk_size", 1000), sum_columns=kw.pop("sum_columns", ["ORDER_TOTAL"]), **kw)


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def keys(client, prefix):
    resp = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"] for o in resp.get("Contents", []))


def read_partition(client, prefix):
    tables = []
    for k in keys(client, prefix):
        if k.endswith(".parquet"):
            body = client.get_object(Bucket=BUCKET, Key=k)["Body"].read()
            tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables) if tables else None


PART = "raw/oracle/northwind/orders/load_date=2026-10-08/"
NOW = dt.datetime(2026, 10, 8, 12, 0, 0)


def test_full_load_types_and_values(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    audit = run_table(cfg("full", chunk_size=1000), ENV, FakeSource(make_rows(2500), NOW), target, "2026-10-08", "r1")
    assert audit["status"] == "SUCCESS", audit.get("error")
    assert audit["rows_written"] == 2500 and len(audit["files"]) == 3
    assert audit["sums"]["ORDER_TOTAL"]["match"]
    t = read_partition(s3, PART)
    assert t.num_rows == 2500
    schema = {f.name: f.type for f in t.schema}
    assert schema["ORDER_ID"] == pa.int64()
    assert schema["ORDER_DATE"] == pa.timestamp("us")               # DATE keeps its time
    assert schema["ORDER_TOTAL"] == pa.decimal128(38, 10)           # no rounding
    assert schema["NOTES"] == pa.string()
    first = t.sort_by("ORDER_ID").slice(0, 5).to_pylist()
    assert first[0]["ORDER_TOTAL"] == Decimal("1235.56789")
    assert first[0]["ORDER_DATE"] == dt.datetime(2026, 1, 1, 10, 1)
    assert first[4]["NOTES"] == 'gift wrap, "fragile"\nring twice'  # commas, quotes, newline intact
    assert "_batch_id" in schema and "_ingested_at" in schema
    assert keys(s3, "_control/audit/orders/")                      # audit record written


def test_full_rerun_replaces_snapshot(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    run_table(cfg("full"), ENV, FakeSource(make_rows(300), NOW), target, "2026-10-08", "r1")
    run_table(cfg("full"), ENV, FakeSource(make_rows(320), NOW), target, "2026-10-08", "r2")
    t = read_partition(s3, PART)
    assert t.num_rows == 320                                        # not 620: idempotent
    assert all(k.split("/")[-1].startswith("full_r2") for k in keys(s3, PART))


def test_incremental_loads_only_new_rows(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    rows = make_rows(100)
    a1 = run_table(cfg("incremental"), ENV, FakeSource(rows, NOW), target, "2026-10-08", "r1")
    assert a1["status"] == "SUCCESS" and a1["rows_written"] == 100
    wm1 = target.read_watermark("orders")["last_watermark"]

    new = make_rows(7, start_id=1000, ts=NOW)                         # changed after first run
    later = NOW + dt.timedelta(hours=20)
    a2 = run_table(cfg("incremental"), ENV, FakeSource(rows + new, later), target, "2026-10-08", "r2")
    assert a2["status"] == "SUCCESS" and a2["rows_written"] == 7
    assert read_partition(s3, PART).num_rows == 107
    assert target.read_watermark("orders")["last_watermark"] > wm1

    a3 = run_table(cfg("incremental"), ENV, FakeSource(rows + new, later), target, "2026-10-08", "r3")
    assert a3["status"] == "SUCCESS" and a3["rows_written"] == 0      # nothing new, no duplicates
    assert read_partition(s3, PART).num_rows == 107


def test_count_mismatch_rolls_back(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    audit = run_table(cfg("incremental"), ENV, FakeSource(make_rows(50), NOW, count_offset=1),
                      target, "2026-10-08", "r1")
    assert audit["status"] == "FAILED" and "row count" in audit["error"]
    assert [k for k in keys(s3, PART) if k.endswith(".parquet")] == []   # rolled back
    assert target.read_watermark("orders") is None                      # watermark untouched


def test_too_many_decimals_fails_loudly(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    rows = make_rows(3)
    rows[1] = rows[1][:3] + (Decimal("1.123456789012"),) + rows[1][4:]
    audit = run_table(cfg("full"), ENV, FakeSource(rows, NOW), target, "2026-10-08", "r1")
    assert audit["status"] == "FAILED" and "ORDER_TOTAL" in audit["error"]
    assert keys(s3, PART) == []


def test_previous_snapshot_survives_failed_full_rerun(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    run_table(cfg("full"), ENV, FakeSource(make_rows(40), NOW), target, "2026-10-08", "r1")
    bad = run_table(cfg("full"), ENV, FakeSource(make_rows(45), NOW, count_offset=3), target, "2026-10-08", "r2")
    assert bad["status"] == "FAILED"
    assert read_partition(s3, PART).num_rows == 40                    # yesterday's good copy kept


def test_dry_run_writes_nothing(s3):
    target = S3Target(BUCKET, "us-east-1", client=s3)
    audit = run_table(cfg("full"), ENV, FakeSource(make_rows(10), NOW), target, "2026-10-08", "r1", dry_run=True)
    assert audit["status"] == "DRY_RUN" and audit["source_count"] == 10
    assert keys(s3, "") == []


def write_yaml(tmp_path, name, text):
    p = tmp_path / f"{name}.yaml"
    p.write_text(text)
    return p


GOOD = """
table_id: {tid}
source: {{schema: NORTHWIND, table: {table}}}
load: {{type: {lt} {extra}}}
target: {{prefix: raw/oracle/northwind/x}}
"""


def test_config_rejects_sql_injection(tmp_path):
    p = write_yaml(tmp_path, "bad", GOOD.format(tid="bad", table="'ORDERS; DROP TABLE X'", lt="full", extra=""))
    with pytest.raises(ConfigError, match="invalid source.table"):
        load_table(p)


def test_config_incremental_needs_watermark(tmp_path):
    p = write_yaml(tmp_path, "inc", GOOD.format(tid="inc", table="ORDERS", lt="incremental", extra=""))
    with pytest.raises(ConfigError, match="watermark_column"):
        load_table(p)


def test_config_file_name_must_match(tmp_path):
    p = write_yaml(tmp_path, "other_name", GOOD.format(tid="orders", table="ORDERS", lt="full", extra=""))
    with pytest.raises(ConfigError, match="file name must match"):
        load_table(p)


def test_repo_configs_are_valid():
    from dif.config import load_all_tables
    tables = load_all_tables(Path(__file__).resolve().parents[1] / "config")
    assert len(tables) >= 5
