# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d812765-703c-45fa-a54d-f31919cc9bfe",
# META       "default_lakehouse_name": "smart_city_lh",
# META       "default_lakehouse_workspace_id": "903e14be-9926-484e-95f4-7e939a0851d1",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d812765-703c-45fa-a54d-f31919cc9bfe"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run nb_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = get_logger("nb_silver_to_gold_engine")
gold_cfg = GOLD["smart_city"]
gold_table = gold_cfg["table"]

rules = gold_cfg["rules"]
ROLL_HOURS = int(rules["rolling_avg_hours"])
OVERLAP_HOURS = int(gold_cfg["merge"].get("recompute_overlap_hours", max(ROLL_HOURS * 2, 6)))
WIND_THRESHOLD = float(rules["wind_threshold"])

log_event(spark, logger, "INFO", "Silver->Gold started", step="start", extra={
    "gold_table": gold_table,
    "rolling_avg_hours": ROLL_HOURS,
    "recompute_overlap_hours": OVERLAP_HOURS,
    "load_mode": RUN.get("load_mode"),
    "run_date": RUN.get("run_date"),
})

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _table_exists(table_name: str) -> bool:
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False

def _get_gold_start_ts() -> "Column | None":
    """
    For incremental: recompute from max(timestamp) - overlap hours.
    If Gold doesn't exist: return None => process all available silver.
    """
    if not _table_exists(gold_table):
        return None

    mx = spark.sql(f"SELECT MAX(timestamp) AS mx FROM {gold_table}").collect()[0]["mx"]
    if mx is None:
        return None

    # start_ts = mx - overlap hours
    return F.expr(f"timestamp('{mx}') - INTERVAL {OVERLAP_HOURS} HOURS")

def _read_silver(path: str, start_ts_expr=None) -> DataFrame:
    df = spark.read.format("delta").load(path)
    df = clean_column_names(df)
    if start_ts_expr is not None:
        df = df.where(F.col("timestamp") >= start_ts_expr)
    return df

def _prefix_non_keys(df: DataFrame, prefix: str, keys=("timestamp",)) -> DataFrame:
    out = df
    for c in df.columns:
        if c not in keys:
            out = out.withColumnRenamed(c, f"{prefix}{c}")
    return out


# ------------------------------------------------------------
# Decide incremental window
# ------------------------------------------------------------
start_ts_expr = None
if RUN.get("load_mode") != "full_refresh":
    start_ts_expr = _get_gold_start_ts()

log_event(spark, logger, "INFO", "Gold recompute window decided", step="window", extra={
    "start_ts_is_none": start_ts_expr is None,
    "overlap_hours": OVERLAP_HOURS,
})

# ------------------------------------------------------------
# Read Silver inputs (Files delta paths)
# ------------------------------------------------------------
p_energy = DATASETS["energy"]["silver"]["path"]
p_weather = DATASETS["weather"]["silver"]["path"]
p_air = DATASETS["air_quality"]["silver"]["path"]

df_energy = _read_silver(p_energy, start_ts_expr=start_ts_expr)
df_weather = _read_silver(p_weather, start_ts_expr=start_ts_expr)
df_air = _read_silver(p_air, start_ts_expr=start_ts_expr)

# Safety: ensure required columns exist
require_columns(df_energy, ["timestamp", "price"], dataset="energy")
require_columns(df_weather, ["timestamp"], dataset="weather")
require_columns(df_air, ["timestamp"], dataset="air_quality")

# Keep Silver minimal columns for join (ignore extra)
df_energy = select_columns_safe(df_energy, ["timestamp", "price"])
df_weather = df_weather.dropDuplicates(["timestamp"])
df_air = df_air.dropDuplicates(["timestamp"])

# Prefix right-side columns to avoid collisions
df_weather = _prefix_non_keys(df_weather, "wx_", keys=("timestamp",))
df_air = _prefix_non_keys(df_air, "aq_", keys=("timestamp",))

log_event(spark, logger, "INFO", "Silver inputs loaded", step="read_done", extra={
    "paths": {"energy": p_energy, "weather": p_weather, "air_quality": p_air},
})

# ------------------------------------------------------------
# Join (Energy is the driving time series)
# ------------------------------------------------------------
df = (
    df_energy
    .join(df_weather, on="timestamp", how="left")
    .join(df_air, on="timestamp", how="left")
)

# ------------------------------------------------------------
# Rolling average (range-based window, robust to missing hours)
# avg over previous ROLL_HOURS hours excluding current row
# ------------------------------------------------------------
ts_long = F.col("timestamp").cast("long")
w = Window.orderBy(ts_long).rangeBetween(-(ROLL_HOURS * 3600), -1)

df = df.withColumn("avg_price_last_3h", F.avg(F.col("price")).over(w))

df = df.withColumn(
    "pct_vs_3h_avg",
    F.when(
        F.col("avg_price_last_3h").isNotNull() & (F.col("avg_price_last_3h") != 0),
        (F.col("price") - F.col("avg_price_last_3h")) / F.col("avg_price_last_3h")
    )
)

# Business label
df = df.withColumn(
    "label",
    F.when(
        (F.col("avg_price_last_3h").isNotNull()) &
        (F.col("price") < F.col("avg_price_last_3h")) &
        (F.col("wx_wind_speed_10m").isNotNull()) &
        (F.col("wx_wind_speed_10m") >= F.lit(WIND_THRESHOLD)),
        F.lit(rules["label_true"])
    ).otherwise(F.lit(rules["label_false"]))
)

# Final column list (from config if present)
final_cols = gold_cfg.get("select", None)
if final_cols:
    df_gold = select_columns_safe(df, final_cols)
else:
    df_gold = df

# Enforce non-null timestamp
df_gold = df_gold.filter(F.col("timestamp").isNotNull()).dropDuplicates(["timestamp"])

# ------------------------------------------------------------
# Write Gold
# full_refresh => overwrite table
# incremental => MERGE on timestamp
# ------------------------------------------------------------
if RUN.get("load_mode") == "full_refresh":
    log_event(spark, logger, "INFO", "Writing Gold (full_refresh overwrite)", step="write")
    (df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table))
else:
    log_event(spark, logger, "INFO", "Writing Gold (incremental merge)", step="write",
              extra={"merge_keys": gold_cfg["merge"]["key_cols"]})
    merge_into_table(
        spark=spark,
        df=df_gold,
        table_name=gold_table,
        key_cols=gold_cfg["merge"]["key_cols"],
        mode_if_missing="overwrite",
    )

# Optional: row count logging (comment out if you want faster)
rows = df_gold.count()
log_event(spark, logger, "INFO", "Gold write complete", step="write_done", extra={"rows": rows})

log_event(spark, logger, "INFO", "Silver->Gold finished", step="end")


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

# CELL ********************

df = spark.sql("SELECT * FROM smart_city_lh.dbo.gold_smart_city_monitoring LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM smart_city_lh.dbo.ops_log_events LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
