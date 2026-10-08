"""The command line, end to end, with fake Oracle + fake S3."""
import datetime as dt
import shutil
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

import dif.oracle_source
from dif import run
from tests.test_pipeline import FakeSource, make_rows

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    cfg = tmp_path / "config"
    shutil.copytree(ROOT / "config", cfg)
    env = cfg / "env" / "dev.yaml"
    env.write_text(env.read_text().replace("<your-bucket-name>", "cli-bucket"))
    for p in (cfg / "tables").glob("*.yaml"):          # keep one table, with matching columns
        if p.stem != "northwind_orders":
            p.unlink()

    fake = FakeSource(make_rows(1234), dt.datetime(2026, 10, 8, 12))

    class FakeOracle:
        def __init__(self, *a, **k):
            pass

        def __enter__(self):
            return fake

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(dif.oracle_source, "OracleSource", FakeOracle)
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="cli-bucket")
        yield cfg, tmp_path / "logs"


def test_cli_all_then_rerun(workspace):
    cfg, logs = workspace
    args = ["--env", "dev", "--config-dir", str(cfg), "--log-dir", str(logs), "--load-date", "2026-10-08"]
    assert run.main(args + ["--all"]) == 0
    assert run.main(args + ["--table", "northwind_orders"]) == 0       # nothing new, still OK
    assert any(logs.glob("dif_*.log"))
    s3 = boto3.client("s3", region_name="us-east-1")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="cli-bucket")["Contents"]]
    assert any(k.startswith("_control/watermarks/northwind_orders") for k in keys)
    assert sum(k.endswith(".parquet") for k in keys) == 1


def test_cli_unknown_table_is_usage_error(workspace):
    cfg, logs = workspace
    assert run.main(["--env", "dev", "--config-dir", str(cfg), "--log-dir", str(logs), "--table", "nope"]) == 2


def test_cli_reset_watermark(workspace):
    cfg, logs = workspace
    base = ["--env", "dev", "--config-dir", str(cfg), "--log-dir", str(logs)]
    assert run.main(base + ["--table", "northwind_orders"]) == 0
    assert run.main(base + ["--table", "northwind_orders", "--reset-watermark"]) == 0
    s3 = boto3.client("s3", region_name="us-east-1")
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket="cli-bucket")["Contents"]]
    assert not any(k.startswith("_control/watermarks/") for k in keys)
