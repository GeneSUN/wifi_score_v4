# ============================================================
# 🧠 MIND MAP — Workflow Overview of This Script
# ============================================================
#
# 1️⃣  Initialization Phase
# ├── Import necessary libraries (PySpark, datetime, argparse, custom modules)
# ├── Append system path to include HourlyScore modules
# ├── Initialize SparkSession
# └── Define HDFS paths:
#       • station_history_path
#       • station_connection_path
#       • station_score_output_path
#       • wifi_score_output_path
#
# 
# 2️⃣  Argument Parsing
# ├── Use argparse to receive "--date_list" from shell script
# ├── If provided → split into list of date_str values
# └── If not provided → automatically define:
#       • yesterday = today - 1 day
#       • today = current date
#       → date_str_list = [yesterday, today]
#
#
# 3️⃣  Missing Date/Hour Detection
# ├── For each date_str in date_str_list:
# │     ├── Call find_missing_date_hours()
# │     │     ├── Determine which hours to check:
# │     │     │     • If date_str == today → check only hours < current hour
# │     │     │     • Else → check full 00–23 hours
# │     │     ├── For each hour:
# │     │     │     ├── Construct both HDFS paths:
# │     │     │     │     • station_score_output_path/date=.../hour=...
# │     │     │     │     • wifi_score_output_path/date=.../hour=...
# │     │     │     ├── Check if each path exists via Hadoop FileSystem
# │     │     │     ├── If both exist → log “[OK]”
# │     │     │     └── If either missing → record in missing_list:
# │     │     │           {date_str, hour_str, missing=[which ones missing]}
# │     │     └── Return missing_list for that date
# │     └── Collect all missing results into all_missing[]
#
#
# 4️⃣  Reprocessing Phase (for missing hours)
# ├── For each record in all_missing:
# │     ├── Extract date_str, hour_str, missing
# │     ├── Step 1: StationConnectionProcessor
# │     │     • Input: station_history_path, date_str, hour_str
# │     │     • Output: station_connection_df (cached)
# │     ├── Step 2: station_score_hourly
# │     │     • Input: station_connection_df
# │     │     • Output written to station_score_output_path
# │     └── Step 3: wifi_score_hourly
# │           • Input: same station_connection_df
# │           • Output written to wifi_score_output_path
#
#
# 5️⃣  Finalization
# ├── Print “[DONE] All missing hours have been reprocessed successfully.”
# └── End Spark job
#
#
# 💡 Summary of Logic Flow
# ------------------------------------------------------------
# Shell → passes optional --date_list
# Python → decides which dates to check (today/yesterday fallback)
# For each date → find missing hourly outputs (station & wifi)
# For missing hours → regenerate connection → run scoring jobs
# Outputs written back to respective HDFS hourly directories
# ============================================================


from datetime import datetime, timedelta
import argparse
import sys
import time
import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructType,
    Row,
)

# --------------------------------------------------------
# Custom Imports
# --------------------------------------------------------
sys.path.append('/usr/apps/vmas/script/ZS/HourlyScore')
from StationConnection import StationConnectionProcessor, LogTime
from station_score_hourly import station_score_hourly
from wifi_score_hourly import wifi_score_hourly


# ============================================================
# Function: Find Missing Hours for Both Station & WiFi Outputs
# ============================================================
def find_missing_date_hours(
    spark,
    date_str: str,
    station_score_output_path: str,
    wifi_score_output_path: str,
):
    """
    Identify which (date, hour) combinations have missing station_score or wifi_score outputs.

    Returns:
        List of dicts with keys: 'date_str', 'hour_str', 'missing'
    """
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())

    # Determine which hours to check
    today_str = ( (datetime.utcnow()-timedelta(hours=1) ) ).strftime("%Y%m%d")
    if date_str == today_str:
        current_hour = (datetime.utcnow() - timedelta(hours=1)).hour
        hours_to_check = [f"{h:02d}" for h in range(current_hour)]  # only past hours
    else:
        hours_to_check = [f"{h:02d}" for h in range(24)]  # full 24 hours

    print(f"[INFO] Checking {len(hours_to_check)} hour folders for date={date_str}")

    missing_list = []
    for hour_str in hours_to_check:
        station_hour_path = f"{station_score_output_path}/date={date_str}/hour={hour_str}"
        wifi_hour_path = f"{wifi_score_output_path}/date={date_str}/hour={hour_str}"

        station_path_obj = spark._jvm.org.apache.hadoop.fs.Path(station_hour_path)
        wifi_path_obj = spark._jvm.org.apache.hadoop.fs.Path(wifi_hour_path)

        station_exists = hadoop_fs.exists(station_path_obj)
        wifi_exists = hadoop_fs.exists(wifi_path_obj)

        if not (station_exists and wifi_exists):
            missing = []
            if not station_exists:
                missing.append("station_score")
            if not wifi_exists:
                missing.append("wifi_score")

            missing_list.append({
                "date_str": date_str,
                "hour_str": hour_str,
                "missing": missing
            })
            print(f"[MISSING] {missing} for date={date_str}, hour={hour_str}")
        else:
            print(f"[OK] Both exist: date={date_str}, hour={hour_str}")

    return missing_list


# ============================================================
# Argument Parser
# ============================================================
def parse_args():
    parser = argparse.ArgumentParser(description="Hourly Score Checker")
    parser.add_argument(
        "--date_list",
        type=str,
        help='Space-separated list of dates, e.g. "20261015 20261016"',
        default=None,
    )
    return parser.parse_args()


# ============================================================
# Main Entry Point
# ============================================================
if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("backfill_wifi_score_hourly_report_v1")
        .config("spark.ui.port", "24045")
        .getOrCreate()
    )

    # HDFS Path Configuration
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000"
    station_history_path = f"{hdfs_pa}/sha_data/StationHistory"
    device_groups_path = f"{hdfs_pa}/sha_data/DeviceGroups"
    station_connection_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_connection_hourly"
    station_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_score_hourly"
    wifi_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/wifi_score_hourly"


    # Parse arguments
    args = parse_args()

    # Determine which dates to process
    if args.date_list:
        date_str_list = args.date_list.split()
        print(f"[INFO] Using provided date list: {date_str_list}")
    else:
        current_utc_hour =  (datetime.utcnow()-timedelta(hours=1) )
        today = (current_utc_hour).strftime("%Y%m%d")
        yesterday = (current_utc_hour- timedelta(days=1)).strftime("%Y%m%d")
        day_before_yesterday = (current_utc_hour- timedelta(days=2)).strftime("%Y%m%d")
        date_str_list = [yesterday, today]
        #date_str_list = [day_before_yesterday, yesterday, today]
        print(f"[INFO] No date list passed. Using default: {date_str_list}")

    # Find all missing date/hour combinations
    all_missing = []
    for date_str in date_str_list:
        missing_for_date = find_missing_date_hours(
            spark,
            date_str=date_str,
            station_score_output_path=station_score_output_path,
            wifi_score_output_path=wifi_score_output_path,
        )
        all_missing.extend(missing_for_date)

    # Reprocess missing date-hour combinations
    for d in all_missing:
        date_str, hour_str, missing = d["date_str"], d["hour_str"], d["missing"]
        print(f"\n[REPROCESS] date={date_str}, hour={hour_str}, missing={missing}")

        # Step 1: Generate station connection
        with LogTime() as timer:
            processor = StationConnectionProcessor(
                spark,
                input_path=station_history_path,
                date_str=date_str,
                hour_str=hour_str
            )
            station_connection_df = processor.run()
            station_connection_df.cache()

        # Step 2: Run station score
        with LogTime() as timer:
            station_runner = station_score_hourly(
                spark,
                date_str,
                hour_str,
                station_connection_df,
                output_path=station_score_output_path
            )
            station_runner.run()

        # Step 3: Run WiFi score
        with LogTime() as timer:

            wifi_score_runner = wifi_score_hourly(
                spark=spark,
                date_str=date_str,
                hour_str=hour_str,
                source_df=station_connection_df,
                station_history_path=station_history_path,
                device_groups_path=device_groups_path,
                output_path=wifi_score_output_path
            )
            wifi_score_runner.run()


    print("[DONE] All missing hours have been reprocessed successfully.")
