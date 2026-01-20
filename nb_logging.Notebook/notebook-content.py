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

# nb_logging
# Structured logging to a Lakehouse Delta table (append-only).

import logging, json
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List

LOG_TABLE = "ops_log_events"
LOG_FORMAT = "[%(asctime)s] - [%(name)s] - %(levelname)s - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """Console logger (cell output)."""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if not logger.handlers:
        fmt = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)
        h = logging.StreamHandler()
        h.setLevel(level)
        h.setFormatter(fmt)
        logger.addHandler(h)

    return logger


def _get_run_context() -> Dict[str, str]:
    """
    Best-effort pipeline context (empty in interactive runs).
    """
    ctx = {
        "pipeline_job_id": "",
        "activity_run_id": "",
        "notebook_name": "",
    }
    try:
        import mssparkutils
        rc = mssparkutils.runtime.context
        ctx["pipeline_job_id"] = rc.get("Pipelinejobid", "") or rc.get("pipelinejobid", "") or ""
        ctx["activity_run_id"] = rc.get("Activityrunid", "") or rc.get("activityrunid", "") or ""
        ctx["notebook_name"] = rc.get("Notebookname", "") or rc.get("notebookname", "") or ""
    except Exception:
        pass
    return ctx


def _get_run_date() -> str:
    """
    If nb_config was loaded, tries to read RUN['run_date'].
    Otherwise returns today's UTC date.
    """
    try:
        run = globals().get("RUN", None)
        if isinstance(run, dict) and run.get("run_date"):
            return str(run["run_date"])
    except Exception:
        pass
    return datetime.utcnow().strftime("%Y-%m-%d")


def ensure_log_table(spark) -> None:
    """
    Creates an append-only Delta table for operational logs (if not exists).
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
            event_ts        TIMESTAMP,
            level           STRING,
            logger          STRING,
            notebook_name   STRING,
            pipeline_job_id STRING,
            activity_run_id STRING,
            run_date        STRING,
            dataset         STRING,
            step            STRING,
            message         STRING,
            extra_json      STRING
        )
        USING DELTA
    """)


def log_event(
    spark,
    logger: logging.Logger,
    level: str,
    message: str,
    dataset: str = "",
    step: str = "",
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Writes:
      1) console log (logger)
      2) structured row into ops_log_events (append)
    """
    ensure_log_table(spark)
    ctx = _get_run_context()
    run_date = _get_run_date()

    lvl = level.upper()
    if lvl == "DEBUG": logger.debug(message)
    elif lvl == "INFO": logger.info(message)
    elif lvl == "WARNING": logger.warning(message)
    elif lvl == "ERROR": logger.error(message)
    elif lvl == "CRITICAL": logger.critical(message)
    else: logger.info(message)

    row = [{
        "event_ts": datetime.now(timezone.utc).replace(tzinfo=None),
        "level": lvl,
        "logger": logger.name,
        "notebook_name": ctx["notebook_name"],
        "pipeline_job_id": ctx["pipeline_job_id"],
        "activity_run_id": ctx["activity_run_id"],
        "run_date": run_date,
        "dataset": dataset,
        "step": step,
        "message": message,
        "extra_json": json.dumps(extra or {}, ensure_ascii=False),
    }]

    spark.createDataFrame(row).write.format("delta").mode("append").saveAsTable(LOG_TABLE)


class LogBuffer:
    """
    Buffer multiple events and flush once (reduces small Delta writes).
    Use for chatty steps; keep milestone events as direct log_event calls.
    """
    def __init__(self, spark, logger: logging.Logger):
        self.spark = spark
        self.logger = logger
        self.items: List[Dict[str, Any]] = []

    def add(self, level: str, message: str, dataset: str = "", step: str = "",
            extra: Optional[Dict[str, Any]] = None) -> None:
        ctx = _get_run_context()
        run_date = _get_run_date()

        self.items.append({
            "event_ts": datetime.now(timezone.utc).replace(tzinfo=None),
            "level": level.upper(),
            "logger": self.logger.name,
            "notebook_name": ctx["notebook_name"],
            "pipeline_job_id": ctx["pipeline_job_id"],
            "activity_run_id": ctx["activity_run_id"],
            "run_date": run_date,
            "dataset": dataset,
            "step": step,
            "message": message,
            "extra_json": json.dumps(extra or {}, ensure_ascii=False),
        })

    def flush(self) -> None:
        if not self.items:
            return
        ensure_log_table(self.spark)
        self.spark.createDataFrame(self.items).write.format("delta").mode("append").saveAsTable(LOG_TABLE)
        self.items = []


print("Logging Loaded")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
