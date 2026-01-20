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

# # Config file


# CELL ********************

# nb_config
# Incremental, metadata-driven pipeline config (capsules live here).
# Silver = Delta in Files (MERGE/upsert)
# Gold   = Managed Table (MERGE/upsert)

from datetime import datetime

# -----------------------------
# Project settings
# -----------------------------
PROJECT = {
    "city": "Amsterdam",
    "lat": 52.3676,
    "lon": 4.9041,
    "source_tz": "Europe/Amsterdam",
    "standard_tz": "UTC",
}

# -----------------------------
# Run settings
# -----------------------------
RUN = {
    # In pipelines you can override this with a parameter later
    "run_date": datetime.utcnow().strftime("%Y-%m-%d"),  # "YYYY-MM-DD"
    "load_mode": "incremental",  # "incremental" | "full_refresh"
}

# -----------------------------
# Lakehouse paths
# -----------------------------
ROOT_FILES = "abfss://smart_city_ws@onelake.dfs.fabric.microsoft.com/smart_city_lh.Lakehouse/Files"

LAYERS = {
    "bronze": f"{ROOT_FILES}/bronze",
    "silver": f"{ROOT_FILES}/silver",
    "gold":   f"{ROOT_FILES}/gold",
}

# -----------------------------
# Global write policies
# -----------------------------
SILVER_WRITE = {
    "format": "delta",
    "mode": "append",            # for incremental, we will MERGE (engine will handle)
    "partition_cols": ["date"],  # engine should create a 'date' column from timestamp
}

GOLD_WRITE = {
    "format": "delta",
    "mode": "overwrite",         # engine will prefer MERGE; overwrite can be used for full refresh
}

# -----------------------------
# Hash key policy (surrogate key)
# -----------------------------
HASH = {
    "algo": "md5",
    "delimiter": "||",
    "col_name": "hk",            # hash key column name in Silver/Gold
}

# IMPORTANT NOTE ABOUT KEYS:
# For incremental loads, keys should be based on STABLE identifiers.
# Best practice here is timestamp (+ city/lat/lon if you later support multiple locations).
# Using "price" in the key is not recommended because it changes the key if values are corrected.
#
# If you still want "timestamp + price" for Energy, you can change key_cols in that capsule.

# -----------------------------
# Dataset capsules (Bronze -> Silver)
# -----------------------------
DATASETS = {
    "weather": {
        # Read only today's bronze file for incremental (recommended)
        "bronze": {
            "path_template": f"{LAYERS['bronze']}/weather/{{run_date}}.json",
            "fallback_glob": f"{LAYERS['bronze']}/weather/*.json",  # for backfills / full refresh
            "format": "json",
        },
        "silver": {
            "path": f"{LAYERS['silver']}/weather",  # Delta folder
            "write": {**SILVER_WRITE},
            "merge": {
                "enabled": True,
                "key_cols": ["timestamp"]  # stable key
            }
        },
        "transform": {
            "type": "open_meteo_hourly",
            "params": {
                "hourly_fields": ["time", "temperature_2m", "wind_speed_10m", "direct_radiation"],
                "timestamp_field": "time",
                "source_timezone": PROJECT["source_tz"],
                "to_utc": True
            }
        },
        "select": ["timestamp", "temperature_2m", "wind_speed_10m", "direct_radiation"],
    },

    "air_quality": {
        "bronze": {
            "path_template": f"{LAYERS['bronze']}/air_quality/{{run_date}}.json",
            "fallback_glob": f"{LAYERS['bronze']}/air_quality/*.json",
            "format": "json"
        },
        "silver": {
            "path": f"{LAYERS['silver']}/air_quality",
            "write": {**SILVER_WRITE},
            "merge": {
                "enabled": True,
                "key_cols": ["timestamp"]
            },
        },
        "transform": {
            "type": "open_meteo_hourly",
            "params": {
                "hourly_fields": ["time", "carbon_monoxide", "nitrogen_dioxide", "pm10"],
                "timestamp_field": "time",
                "source_timezone": PROJECT["source_tz"],
                "to_utc": True
            }
        },
        "select": ["timestamp", "carbon_monoxide", "nitrogen_dioxide", "pm10"],
    },

    "energy": {
        "bronze": {
            "path_template": f"{LAYERS['bronze']}/energy/{{run_date}}.json",
            "fallback_glob": f"{LAYERS['bronze']}/energy/*.json",
            "format": "json"
        },
        "silver": {
            "path": f"{LAYERS['silver']}/energy",
            "write": {**SILVER_WRITE},
            "merge": {
                "enabled": True,
                # Recommended stable key:
                "key_cols": ["timestamp"]
                # If you INSIST on timestamp+price hash, change to:
                # "key_cols": ["timestamp", "price"],
            }
        },
        "transform": {
            "type": "explode_array_of_struct",
            "params": {
                "array_col": "Prices",
                "element_alias": "price_row",
                "select_map": {
                    "timestamp": "readingDate",
                    "price": "price"
                },
                "timestamp": {
                    "col": "timestamp",
                    "to_timestamp": True,
                    "to_utc": False,  # set True if Energy timestamps are local
                    "source_timezone": PROJECT["source_tz"]
                }
            }
        },
        "select": ["timestamp", "price"]
    }
}

# -----------------------------
# Gold capsule (Silver -> Gold)
# -----------------------------
GOLD = {
    "smart_city": {
        "table": "gold_smart_city_monitoring",
        "write": {**GOLD_WRITE},
        "merge": {
            "enabled": True,
            "key_cols": ["timestamp"],  # stable key for Gold
            # Because Gold has rolling windows, recompute overlap:
            "recompute_overlap_hours": 6 # recompute last N hours + new hours (safe for 3h rolling avg)
        },
        "rules": {
            "rolling_avg_hours": 3,
            "wind_threshold": 6.0,
            "label_true": "CHEAP",
            "label_false": "NORMAL"
        },
        "select": [
            "timestamp",
            "price",
            "avg_price_last_3h",
            "pct_vs_3h_avg",
            "wx_temperature_2m",
            "wx_wind_speed_10m",
            "wx_direct_radiation",
            "aq_carbon_monoxide",
            "aq_nitrogen_dioxide",
            "aq_pm10",
            "label"
        ]
    }
}

print("Config Loaded")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
