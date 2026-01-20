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

# MARKDOWN ********************

# ### ENERGY

# CELL ********************

%run nb_config


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logger = get_logger("nb_bronze_to_silver_engine")
log_event(spark, logger, "INFO", "Bronze->Silver started", step="start", extra={
    "run_date": RUN.get("run_date"),
    "load_mode": RUN.get("load_mode")
})

def _read_bronze(dataset_name: str, ds_cfg: dict) -> "DataFrame":
    """
    Incremental: try path_template (run_date.json).
    Full refresh or fallback: read fallback_glob.
    """
    bronze_cfg = ds_cfg["bronze"]
    run_date = RUN["run_date"]

    path_today = bronze_cfg["path_template"].format(run_date=run_date)
    glob_all = bronze_cfg.get("fallback_glob")

    # If full refresh: prefer glob
    if RUN.get("load_mode") == "full_refresh" and glob_all:
        log_event(spark, logger, "INFO", "Reading Bronze (full_refresh glob)", dataset=dataset_name, step="read",
                  extra={"path": glob_all})
        return spark.read.format(bronze_cfg.get("format", "json")).load(glob_all)

    # Incremental: try today's file first
    try:
        log_event(spark, logger, "INFO", "Reading Bronze (incremental)", dataset=dataset_name, step="read",
                  extra={"path": path_today})
        return spark.read.format(bronze_cfg.get("format", "json")).load(path_today)
    except Exception as e:
        if not glob_all:
            raise
        log_event(spark, logger, "WARNING", "Today's Bronze file not found; falling back to glob",
                  dataset=dataset_name, step="read_fallback",
                  extra={"path_today": path_today, "fallback_glob": glob_all, "error": str(e)})
        return spark.read.format(bronze_cfg.get("format", "json")).load(glob_all)


def _delta_exists(path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def _create_delta(df: "DataFrame", path: str, partition_cols=None, overwrite_schema=True):
    w = df.write.format("delta").mode("overwrite")
    if overwrite_schema:
        w = w.option("overwriteSchema", "true")
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.save(path)


# ------------------------------------------------------------
# Main engine loop
# ------------------------------------------------------------
for ds_name, ds_cfg in DATASETS.items():
    silver_cfg = ds_cfg["silver"]
    silver_path = silver_cfg["path"]

    merge_cfg = silver_cfg.get("merge", {})
    merge_enabled = bool(merge_cfg.get("enabled", True))
    key_cols = merge_cfg.get("key_cols", ["timestamp"])

    # 1) Read Bronze
    df_raw = _read_bronze(ds_name, ds_cfg)

    # 2) Transform (capsule-driven)
    df = apply_transform(df_raw, ds_cfg["transform"])

    # 3) Standardize columns
    df = clean_column_names(df)

    # 4) Keep only selected business columns (but ensure key columns exist for hashing)
    #    We will ALWAYS add: date partition + hash key.
    select_cols = ds_cfg.get("select", [])
    required_for_key = list(set([c.lower() for c in key_cols]))
    required_for_key = [c for c in required_for_key if c != HASH["col_name"]]  # avoid recursion

    # Ensure key columns exist before hashing
    require_columns(df, required_for_key, dataset=ds_name)

    # 5) Add date partition + hash key (hk)
    df = add_date_partition(df, ts_col="timestamp", date_col="date")
    df = add_hash_key(df, key_cols=required_for_key, hk_col=HASH["col_name"], delimiter=HASH["delimiter"])

    # 6) Final select (ensure timestamp + hk + date are kept)
    final_cols = list(dict.fromkeys(select_cols + ["date", HASH["col_name"]]))  # preserve order, unique
    df = select_columns_safe(df, final_cols)

    # 7) Basic data quality
    df = df.filter(F.col("timestamp").isNotNull())
    df = df.dropDuplicates([HASH["col_name"]])  # protect from duplicate rows in same run

    # 8) Write Silver (Files only)
    write_cfg = silver_cfg.get("write", {})
    partition_cols = write_cfg.get("partition_cols", None)

    if RUN.get("load_mode") == "full_refresh":
        log_event(spark, logger, "INFO", "Writing Silver (full_refresh overwrite)", dataset=ds_name, step="write",
                  extra={"path": silver_path, "partition_cols": partition_cols})
        _create_delta(
            df,
            silver_path,
            partition_cols=partition_cols,
            overwrite_schema=write_cfg.get("overwrite_schema", True),
        )
    else:
        # Incremental MERGE on hash key (hk) by default
        merge_keys = [HASH["col_name"]] if merge_enabled else []
        log_event(spark, logger, "INFO", "Writing Silver (incremental merge)", dataset=ds_name, step="write",
                  extra={"path": silver_path, "merge_keys": merge_keys, "partition_cols": partition_cols})

        if not _delta_exists(silver_path):
            # First time create: write partitioned Delta
            _create_delta(
                df,
                silver_path,
                partition_cols=partition_cols,
                overwrite_schema=True,
            )
        else:
            # Upsert into existing Delta
            merge_into_delta_path(
                spark=spark,
                df=df,
                target_path=silver_path,
                key_cols=merge_keys,
                mode_if_missing="overwrite",
            )

    # 9) Log row count (optional; comment out if you want faster runs)
    rows = df.count()
    log_event(spark, logger, "INFO", "Silver write complete", dataset=ds_name, step="write_done",
              extra={"rows": rows, "silver_path": silver_path})

log_event(spark, logger, "INFO", "Bronze->Silver finished", step="end")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger = get_logger("nb_generic_bronze_to_silver")

log_event(spark, logger, "INFO", "Bronze->Silver started", step="start")

# --- Dataset-specific normalizers (Energy explicit, Weather/Air use your function) ---
from pyspark.sql import functions as F

def normalize_energyzero_prices(df_raw):
    return (
        df_raw
        .withColumn("price_row", F.explode(F.col("Prices")))
        .select(
            F.to_timestamp(F.col("price_row.readingDate")).alias("timestamp"),
            F.col("price_row.price").cast("double").alias("price")
        )
    )

# --- Capsules (rules, not code duplication) ---
capsules = [
    {
        "name": "weather",
        "source": GLOB_BRONZE_WEATHER,
        "transform": lambda df: normalize_open_meteo_hourly(
            df,
            hourly_fields=["time", "temperature_2m", "wind_speed_10m", "direct_radiation"],
            source_timezone=TIMEZONE,   # from nb_config
            to_utc=True
        ),
        "post_select": ["timestamp", "temperature_2m", "wind_speed_10m", "direct_radiation"],  # keep Silver lean
        "dedup_keys": ["timestamp"],
        "target_table": TBL_SILVER_WEATHER
    },
    {
        "name": "air_quality",
        "source": GLOB_BRONZE_AIR_QUALITY,
        "transform": lambda df: normalize_open_meteo_hourly(
            df,
            hourly_fields=["time", "carbon_monoxide", "nitrogen_dioxide", "pm10"],
            source_timezone=TIMEZONE,   # from nb_config
            to_utc=True
        ),
        "post_select": ["timestamp", "carbon_monoxide", "nitrogen_dioxide", "pm10"],
        "dedup_keys": ["timestamp"],
        "target_table": TBL_SILVER_AIR_QUALITY
    },
    {
        "name": "energy",
        "source": GLOB_BRONZE_ENERGY,
        "transform": normalize_energyzero_prices,
        "post_select": ["timestamp", "price"],
        "dedup_keys": ["timestamp"],
        "target_table": TBL_SILVER_ENERGY
    }
]

# --- Engine loop ---
for cap in capsules:
    ds = cap["name"]
    log_event(spark, logger, "INFO", f"Reading Bronze {ds}", dataset=ds, step="read", extra={"source": cap["source"]})

    df_raw = spark.read.json(cap["source"])

    df = cap["transform"](df_raw)
    df = clean_column_names(df)

    # Keep only desired columns safely (won't crash if you tweak later)
    df = select_columns_safe(df, cap["post_select"])

    # Deduplicate if reading multiple JSON snapshots
    if cap.get("dedup_keys"):
        df = df.dropDuplicates(cap["dedup_keys"])

    # Basic sanity check: require timestamp
    if "timestamp" not in df.columns:
        log_event(spark, logger, "ERROR", "Missing required column: timestamp", dataset=ds, step="validate")
        raise ValueError(f"{ds}: missing required column 'timestamp'")

    # Write Silver table (this will also log success/failure via your enhanced save_to_lakehouse)
    save_to_lakehouse(
        df,
        table_name=cap["target_table"],
        mode="overwrite",
        spark_session=spark,
        logger=logger,
        dataset=ds,
        step="silver_write",
        log_rows=True
    )

log_event(spark, logger, "INFO", "Bronze->Silver finished", step="end")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }
