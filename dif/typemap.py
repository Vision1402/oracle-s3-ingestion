"""Oracle column types -> Parquet (Arrow) types.

Rules (from the assessment in the Northwind project):
  * DATE holds a time of day      -> timestamp, never a plain date
  * NUMBER(p,0), p <= 18          -> int64
  * NUMBER(p,s)                   -> decimal(p,s)       (exact, no rounding)
  * NUMBER with no precision      -> decimal(38,10)     (fails loudly if a value
                                                         has more than 10 decimals)
  * VARCHAR2 / CHAR / CLOB        -> string (UTF-8)
Anything unknown becomes a string, so a load never crashes on an odd type,
and the column shows up in the log as a warning to review.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

import pyarrow as pa

log = logging.getLogger(__name__)

STRING_TYPES = {"DB_TYPE_VARCHAR", "DB_TYPE_NVARCHAR", "DB_TYPE_CHAR", "DB_TYPE_NCHAR",
                "DB_TYPE_LONG", "DB_TYPE_CLOB", "DB_TYPE_NCLOB", "DB_TYPE_ROWID"}
TIMESTAMP_TYPES = {"DB_TYPE_DATE", "DB_TYPE_TIMESTAMP"}
UNBOUNDED_NUMBER = pa.decimal128(38, 10)


@dataclass
class Column:
    name: str
    oracle_type: str
    precision: int | None
    scale: int | None
    arrow_type: pa.DataType
    convert: Callable[[Any], Any]


def _identity(v):
    return v


def _to_int(v):
    return None if v is None else int(v)


def _to_str(v):
    return None if v is None else str(v)


def _to_decimal(v):
    if v is None or isinstance(v, Decimal):
        return v
    return Decimal(str(v))


def map_column(name: str, type_name: str, precision: int | None, scale: int | None) -> Column:
    """Map one Oracle column description to an Arrow type + value converter."""
    if type_name == "DB_TYPE_NUMBER":
        if precision and scale == 0 and precision <= 18:
            return Column(name, type_name, precision, scale, pa.int64(), _to_int)
        if precision and scale is not None and 0 <= scale <= precision <= 38:
            return Column(name, type_name, precision, scale, pa.decimal128(precision, scale), _to_decimal)
        # NUMBER with no declared precision (Oracle reports precision 0, scale -127)
        return Column(name, type_name, precision, scale, UNBOUNDED_NUMBER, _to_decimal)
    if type_name in ("DB_TYPE_BINARY_FLOAT", "DB_TYPE_BINARY_DOUBLE"):
        return Column(name, type_name, precision, scale, pa.float64(), _identity)
    if type_name in TIMESTAMP_TYPES:
        return Column(name, type_name, precision, scale, pa.timestamp("us"), _identity)
    if type_name in STRING_TYPES:
        return Column(name, type_name, precision, scale, pa.string(), _to_str)
    log.warning("Column %s has unmapped Oracle type %s; storing as string", name, type_name)
    return Column(name, type_name, precision, scale, pa.string(), _to_str)


def columns_from_description(description) -> list[Column]:
    """Build Column objects from a DB-API cursor.description."""
    cols = []
    for d in description:
        type_code = d[1]
        type_name = getattr(type_code, "name", str(type_code))
        cols.append(map_column(d[0], type_name, d[4], d[5]))
    return cols


def rows_to_table(columns: list[Column], rows: list[tuple], extra: dict[str, tuple[pa.DataType, Any]]) -> pa.Table:
    """Turn a chunk of rows into an Arrow table, plus constant audit columns."""
    arrays, names = [], []
    for idx, col in enumerate(columns):
        values = [col.convert(r[idx]) for r in rows]
        try:
            arrays.append(pa.array(values, type=col.arrow_type))
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            raise ValueError(f"Column {col.name} ({col.oracle_type} -> {col.arrow_type}): {exc}") from exc
        names.append(col.name)
    for name, (arrow_type, value) in extra.items():
        arrays.append(pa.array([value] * len(rows), type=arrow_type))
        names.append(name)
    return pa.Table.from_arrays(arrays, names=names)
