"""Writing to the S3 raw layer and the framework's control files.

Layout:
  s3://<bucket>/<prefix>/load_date=YYYY-MM-DD/<batch_id>-part-00000.parquet
  s3://<bucket>/_control/watermarks/<table_id>.json
  s3://<bucket>/_control/audit/<table_id>/load_date=YYYY-MM-DD/<run_id>.json

Idempotency (safe re-runs):
  * full load        -> batch_id "full"; the whole day's partition is replaced
  * incremental load -> batch_id = the watermark window; a re-run of the same
                        window replaces only its own files
"""
from __future__ import annotations

import io
import json
import logging

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)


class S3Target:
    def __init__(self, bucket: str, region: str, control_prefix: str = "_control", client=None):
        self.bucket = bucket
        self.control_prefix = control_prefix
        self.s3 = client or boto3.client("s3", region_name=region)

    # ---------- data files ----------
    @staticmethod
    def partition_prefix(prefix: str, load_date: str) -> str:
        return f"{prefix}/load_date={load_date}/"

    def delete_prefix(self, key_prefix: str) -> int:
        """Delete every object whose key starts with key_prefix. Returns the count."""
        deleted = 0
        paginator = self.s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=key_prefix):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            for i in range(0, len(keys), 1000):
                batch = keys[i:i + 1000]
                self.s3.delete_objects(Bucket=self.bucket, Delete={"Objects": batch, "Quiet": True})
                deleted += len(batch)
        return deleted

    def write_parquet(self, table: pa.Table, key: str) -> int:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        size = buf.tell()
        buf.seek(0)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buf.getvalue())
        log.info("  wrote s3://%s/%s (%d rows, %.1f KB)", self.bucket, key, table.num_rows, size / 1024)
        return size

    # ---------- control files ----------
    def _put_json(self, key: str, payload: dict) -> None:
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")

    def _get_json(self, key: str) -> dict | None:
        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        except self.s3.exceptions.NoSuchKey:
            return None
        return json.loads(obj["Body"].read())

    def read_watermark(self, table_id: str) -> dict | None:
        return self._get_json(f"{self.control_prefix}/watermarks/{table_id}.json")

    def write_watermark(self, table_id: str, payload: dict) -> None:
        self._put_json(f"{self.control_prefix}/watermarks/{table_id}.json", payload)

    def delete_watermark(self, table_id: str) -> None:
        self.s3.delete_object(Bucket=self.bucket, Key=f"{self.control_prefix}/watermarks/{table_id}.json")

    def write_audit(self, table_id: str, load_date: str, run_id: str, payload: dict) -> str:
        key = f"{self.control_prefix}/audit/{table_id}/load_date={load_date}/{run_id}.json"
        self._put_json(key, payload)
        return key
