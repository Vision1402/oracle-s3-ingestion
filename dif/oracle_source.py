"""Reading from Oracle.

Two details that matter in real migrations:

1. READ ONLY transaction. Every query in one run (the row count, the sums
   and the extract itself) sees the same snapshot of the data, even while
   the application keeps writing. Without it, a count taken before the
   extract can differ from the rows actually extracted.

2. Exact types. Numbers are fetched as Decimal (never float), and CLOBs as
   plain strings, so nothing is rounded or truncated on the way out.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Iterator

import oracledb

log = logging.getLogger(__name__)

oracledb.defaults.fetch_decimals = True   # NUMBER -> Decimal, never float
oracledb.defaults.fetch_lobs = False      # CLOB  -> str


class OracleSource:
    def __init__(self, dsn: str, user_env: str, password_env: str):
        self.dsn = dsn
        self.user = os.environ.get(user_env)
        self.password = os.environ.get(password_env)
        if not self.user or not self.password:
            raise RuntimeError(
                f"Oracle credentials not set: export {user_env} and {password_env} "
                f"(normally in ~/.dif_env on the edge node)"
            )
        self.conn = None

    def __enter__(self) -> "OracleSource":
        log.info("Connecting to Oracle %s as %s", self.dsn, self.user)
        self.conn = oracledb.connect(user=self.user, password=self.password, dsn=self.dsn)
        with self.conn.cursor() as cur:
            cur.execute("SET TRANSACTION READ ONLY")
        return self

    def __exit__(self, *exc) -> None:
        if self.conn is not None:
            self.conn.rollback()      # ends the read-only transaction
            self.conn.close()

    @staticmethod
    def _bind_sizes(cur, binds: dict) -> None:
        # Bind datetimes as TIMESTAMP (default would be DATE and drop fractional seconds,
        # which would make TIMESTAMP(6) watermarks re-read rows).
        sizes = {k: oracledb.DB_TYPE_TIMESTAMP for k, v in binds.items() if isinstance(v, dt.datetime)}
        if sizes:
            cur.setinputsizes(**sizes)

    def scalar(self, sql: str, binds: dict | None = None):
        binds = binds or {}
        with self.conn.cursor() as cur:
            self._bind_sizes(cur, binds)
            cur.execute(sql, binds)
            row = cur.fetchone()
            return row[0] if row else None

    def extract(self, sql: str, binds: dict, chunk_size: int) -> Iterator[tuple[list, list[tuple]]]:
        """Yield (cursor.description, rows) one chunk at a time."""
        with self.conn.cursor() as cur:
            cur.arraysize = min(chunk_size, 10_000)
            cur.prefetchrows = cur.arraysize + 1
            self._bind_sizes(cur, binds)
            cur.execute(sql, binds)
            description = cur.description
            while True:
                rows = cur.fetchmany(chunk_size)
                if not rows:
                    break
                yield description, rows
