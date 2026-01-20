# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# nb_functions
# Generic transforms + incremental Delta merge helpers.

import re
from typing import Dict, Any, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


# -----------------------------
# 1) Column naming + selection
# -----------------------------
def clean_column_names(df: DataFrame) -> DataFrame:
    """Normalize column names to snake_case."""
    out = df
    for c in df.columns:
        n = c.strip().lower()
        n = re.sub(r"[^\w]+", "_", n)
        n = re.sub(r"__+", "_", n).strip("_")
        out = out.withColumnRenamed(c, n)
    return out


def select_columns_safe(df: DataFrame, cols: List[str]) -> DataFrame:
    """Select only existing columns (ignore missing)."""
    keep = [c for c in cols if c in df.columns]
    return df.select(*keep)


def require_columns(df: DataFrame, cols: List[str], dataset: str = "") -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{dataset}: missing required columns: {missing}")


# -----------------------------
# 2) Timestamp parsing + TZ policy
# -----------------------------
def apply_timestamp_policy(
    df: DataFrame,
    col: str,
    to_timestamp: bool = True,
    to_utc: bool = False,
    source_timezone: str = "Europe/Amsterdam"
) -> DataFrame:
    out = df
    if to_timestamp:
        out = out.withColumn(col, F.to_timestamp(F.col(col)))
    if to_utc:
        out = out.withColumn(col, F.to_utc_timestamp(F.col(col), source_timezone))
    return out


def add_date_partition(df: DataFrame, ts_col: str = "timestamp", date_col: str = "date") -> DataFrame:
    """Add YYYY-MM-DD date column derived from timestamp (for partitioning)."""
    return df.withColumn(date_col, F.to_date(F.col(ts_col)))


# -----------------------------
# 3) Hash key generation
# -----------------------------
def add_hash_key(
    df: DataFrame,
    key_cols: List[str],
    hk_col: str = "hk",
    delimiter: str = "||"
) -> DataFrame:
    """
    Deterministic MD5 hash over key_cols.
    Null-safe: null -> ''.
    """
    exprs = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in key_cols]
    return df.withColumn(hk_col, F.md5(F.concat_ws(delimiter, *exprs)))


# -----------------------------
# 4) Transform: open_meteo_hourly
# -----------------------------
def transform_open_meteo_hourly(df_raw: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    Expects df_raw with 'hourly' struct of arrays.
    params:
      - hourly_fields: ["time", ...]
      - timestamp_field: "time"
      - source_timezone: ...
      - to_utc: True/False
    """
    hourly_fields = params["hourly_fields"]
    timestamp_field = params.get("timestamp_field", "time")
    source_timezone = params.get("source_timezone", "Europe/Amsterdam")
    to_utc = params.get("to_utc", True)

    zipped_cols = [F.col(f"hourly.{f}") for f in hourly_fields]
    df_zipped = df_raw.withColumn("_z", F.arrays_zip(*zipped_cols))

    df_rows = df_zipped.select(
        *[c for c in df_raw.columns if c != "hourly"],
        F.explode(F.col("_z")).alias("_r")
    )

    select_cols = [F.col(f"_r.{f}").alias(f) for f in hourly_fields]
    df_out = df_rows.select(*[F.col(c) for c in df_rows.columns if c not in ["_z", "_r"]], *select_cols)

    df_out = df_out.withColumn("timestamp", F.to_timestamp(F.col(timestamp_field)))
    if to_utc:
        df_out = df_out.withColumn("timestamp", F.to_utc_timestamp(F.col("timestamp"), source_timezone))

    df_out = df_out.drop(timestamp_field)
    return df_out


# -----------------------------
# 5) Transform: explode_array_of_struct
# -----------------------------
def transform_explode_array_of_struct(df_raw: DataFrame, params: Dict[str, Any]) -> DataFrame:
    """
    params:
      - array_col: e.g. "Prices"
      - element_alias: e.g. "price_row"
      - select_map: {"timestamp":"readingDate", "price":"price"} (element fields)
      - timestamp: {col, to_timestamp, to_utc, source_timezone} (optional)
    """
    array_col = params["array_col"]
    element_alias = params.get("element_alias", "elem")
    select_map = params["select_map"]

    df = df_raw.withColumn(element_alias, F.explode(F.col(array_col)))

    selects = [F.col(f"{element_alias}.{src}").alias(dst) for dst, src in select_map.items()]
    df = df.select(*selects)

    ts_cfg = params.get("timestamp", None)
    if ts_cfg:
        df = apply_timestamp_policy(
            df,
            col=ts_cfg["col"],
            to_timestamp=ts_cfg.get("to_timestamp", True),
            to_utc=ts_cfg.get("to_utc", False),
            source_timezone=ts_cfg.get("source_timezone", "Europe/Amsterdam"),
        )

    return df


# -----------------------------
# 6) Transform dispatcher
# -----------------------------
def apply_transform(df_raw: DataFrame, transform_cfg: Dict[str, Any]) -> DataFrame:
    t = transform_cfg["type"]
    p = transform_cfg.get("params", {})

    if t == "open_meteo_hourly":
        return transform_open_meteo_hourly(df_raw, p)
    if t == "explode_array_of_struct":
        return transform_explode_array_of_struct(df_raw, p)
    if t == "passthrough":
        return df_raw

    raise ValueError(f"Unknown transform.type: {t}")


# -----------------------------
# 7) Delta MERGE helpers (Files paths)
# -----------------------------
def delta_path_exists(spark, path: str) -> bool:
    """Check if a Delta table exists at a given path."""
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def merge_into_delta_path(
    spark,
    df: DataFrame,
    target_path: str,
    key_cols: List[str],
    mode_if_missing: str = "overwrite",
) -> None:
    """
    Upsert into an existing Delta table at target_path using key_cols.
    If target doesn't exist, create it.
    """
    if not delta_path_exists(spark, target_path):
        (df.write.format("delta").mode(mode_if_missing).save(target_path))
        return

    tgt = DeltaTable.forPath(spark, target_path)

    cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])

    (tgt.alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


# -----------------------------
# 8) MERGE helper for managed table (Gold)
# -----------------------------
def merge_into_table(
    spark,
    df: DataFrame,
    table_name: str,
    key_cols: List[str],
    mode_if_missing: str = "overwrite",
) -> None:
    """
    Upsert into a managed Delta table (Gold) using key_cols.
    If table doesn't exist, create it.
    """
    try:
        tgt = DeltaTable.forName(spark, table_name)
        cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
        (tgt.alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    except Exception:
        (df.write.format("delta").mode(mode_if_missing).saveAsTable(table_name))


print("Functions Loaded")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def normalize_energyzero_prices(df_raw):
    """
    Input schema:
      Prices: array<struct<price:double, readingDate:string>>
      average, fromDate, tillDate, intervalType (top-level)
    Output:
      timestamp (timestamp)
      price (double)
      plus optional metadata columns
    """
    df = (
        df_raw
        .withColumn("price_row", F.explode(F.col("Prices")))
        .select(
            F.to_utc_timestamp(F.col("timestamp"), TIMEZONE), # TIMEZONE = "Europe/Amsterdam"
            F.col("price_row.price").cast("double").alias("price"),
            F.to_timestamp(F.col("fromDate")).alias("from_date"),
            F.to_timestamp(F.col("tillDate")).alias("till_date"),
            F.col("intervalType").cast("long").alias("interval_type"),
            F.col("average").cast("double").alias("average")
        )
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
