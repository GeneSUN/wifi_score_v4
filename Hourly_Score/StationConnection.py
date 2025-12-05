from datetime import datetime, timedelta, date
from functools import reduce
from typing import Optional, List
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,percentile_approx,explode

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructType,
    Row,
)


# Optional but common imports you had:
import numpy as np
import pandas as pd
import sys 
import time
import os

class LogTime:
    def __init__(self, verbose=True, minimum_unit="microseconds") -> None:
        self.minimum_unit = minimum_unit
        self.elapsed = None
        self.verbose = verbose

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.elapsed = time.time() - self.start
        self.elapsed_str = self._format_time(self.elapsed)
        if self.verbose:
            print(f"Time Elapsed: {self.elapsed_str}")

    def _format_time(self, seconds: float) -> str:
        """
        Convert seconds into a human-readable string.
        """
        if seconds < 1e-3:  # less than 1 ms
            return f"{seconds*1e6:.2f} µs"
        elif seconds < 1:   # less than 1 second
            return f"{seconds*1e3:.2f} ms"
        elif seconds < 60:  # less than 1 minute
            return f"{seconds:.2f} s"
        elif seconds < 3600:  # less than 1 hour
            m, s = divmod(seconds, 60)
            return f"{int(m)}m {s:.2f}s"
        else:
            h, r = divmod(seconds, 3600)
            m, s = divmod(r, 60)
            return f"{int(h)}h {int(m)}m {s:.2f}s"


class StationConnectionProcessor:
    # fixed constant
    PERCENTILES = [0.03, 0.1, 0.5, 0.9]

    def __init__(
            self,
            spark: SparkSession,
            date_str: str,
            hour_str: str,
            input_path: str,
            output_path: Optional[str] = None,
        ):
        self.spark = spark
        self.date_str = date_str
        self.hour_str = hour_str
        self.input_path = input_path.rstrip("/")
        self.output_path = output_path.rstrip("/") if output_path else None
        self.stationarity_path = f"/user/ZheS/wifi_score_v4/stationarity/{self.date_str}"


    def compute_stationarity(self):

        try:
            # 2. Attempt to READ the existing data
            print("Attempting to read pre-computed stationarity data...")
            stationary_daily_df = self.spark.read.parquet(self.stationarity_path)
            print("Successfully read existing stationary data.")
            return stationary_daily_df
        except Exception as e:
            print(f"Failed to read existing stationary data: {e}")

            df_sh = self.spark.read.parquet(f"{self.input_path}/date={(datetime.strptime(self.date_str, '%Y%m%d') - timedelta(days=1)).strftime('%Y%m%d')}")

            df = (
                df_sh.withColumn("signal_strength", F.col("Station_Data_connect_data.signal_strength"))
                .filter(F.col("signal_strength").isNotNull())
                .select("Tplg_Data_model_name", "rowkey", "station_data_connect_data.*")
                .withColumn("sn", F.regexp_extract("rowkey", r"-([A-Z0-9]+)_", 1))
            )

            # Window per (sn, station_mac)
            window_spec = Window().partitionBy("sn", "station_mac")

            # Percentiles
            p3 = F.expr(f'percentile_approx(signal_strength, {self.PERCENTILES[0]})').over(window_spec)
            p10 = F.expr(f'percentile_approx(signal_strength, {self.PERCENTILES[1]})').over(window_spec)
            p50 = F.expr(f'percentile_approx(signal_strength, {self.PERCENTILES[2]})').over(window_spec)
            p90 = F.expr(f'percentile_approx(signal_strength, {self.PERCENTILES[3]})').over(window_spec)

            df_outlier = (
                df.withColumn('3%_val', p3)
                .withColumn('10%_val', p10)
                .withColumn('50%_val', p50)
                .withColumn('90%_val', p90)
                .withColumn("lower_bound", F.col('10%_val') - 2 * (F.col('90%_val') - F.col('10%_val')))
                .withColumn("outlier", F.when(F.col("lower_bound") < F.col("3%_val"), F.col("lower_bound")).otherwise(F.col("3%_val")))
                .filter(F.col("signal_strength") > F.col("outlier"))
            )

            stationary_daily_df = (
                df_outlier.withColumn("diff", F.col('90%_val') - F.col('50%_val'))
                .withColumn("stationarity", F.when(F.col("diff") <= 5, F.lit("1")).otherwise(F.lit("0")))
                .groupby("sn", "station_mac").agg(F.max("stationarity").alias("mobility_status"))
                .withColumn(
                    "mobility_status",
                    F.when(F.col("mobility_status") == 1, F.lit("Stationary")).otherwise(F.lit("Non-stationary"))
                )
            )
            stationary_daily_df.write.mode("overwrite").parquet(self.stationarity_path)
            return stationary_daily_df

    def process_hourly(self, stationary_daily_df):
        """Step 2: Process hourly data and join with stationarity."""
        w = Window.partitionBy("station_mac").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        df_conn = (
            self.spark.read.parquet(f"{self.input_path}/date={self.date_str}/hour={self.hour_str}")
            .select("Tplg_Data_model_name", "rowkey", "station_data_connect_data.*")
            .withColumn("sn", F.regexp_extract("rowkey", r"-([A-Z0-9]+)_", 1))
            .withColumn("phy_rate", F.regexp_replace("link_rate", "Mbps", "").cast(IntegerType()))
            .withColumn("tx_phy_rate", F.regexp_replace("tx_link_rate", "Mbps", "").cast(IntegerType()))
            .withColumn("hour", F.date_format(F.from_unixtime(F.col("ts") / 1000), "HH"))
            .withColumn("date", F.date_format(F.from_unixtime(F.col("ts") / 1000), "yyyyMMdd"))
            .select(
                "sn",
                "rowkey",
                "station_mac",
                "station_name",
                F.col("connect_type").alias("band"),
                F.col("signal_strength").alias("rssi"),
                "snr",
                "phy_rate",
                "tx_phy_rate",
                F.col("Tplg_Data_model_name").alias("model"),
                "hour",
                "date"
            )
            .withColumn("p50_rssi", F.percentile_approx("rssi", 0.5).over(w).cast("int"))
            .withColumn("p90_rssi", F.percentile_approx("rssi", 0.9).over(w).cast("int"))

        )

        df_steer = (
            self.spark.read.parquet(f"{self.input_path}/date={self.date_str}/hour={self.hour_str}")
                .select("Tplg_Data_model_name", "rowkey", "station_data_connect_data.*",
                        F.col("Diag_Result_band_steer.sta_type").alias("sta_type_band"),
                        F.col("Diag_Result_band_steer.action").alias("action_band"),
                        F.col("Diag_Result_ap_steer.sta_type").alias("sta_type_ap"),
                        F.col("Diag_Result_ap_steer.action").alias("action_ap"),
                        )\
                .withColumn("sn", F.regexp_extract("rowkey", r"-([A-Z0-9]+)_", 1))\
                .select("sn","station_mac","rowkey","Tplg_Data_model_name","ts","sta_type_band","action_band","sta_type_ap","action_ap")\
                .filter( col("sta_type_band").isNotNull()|col("sta_type_ap").isNotNull() )\
                .groupby("rowkey", "sta_type_band", "action_band")\
                .agg( F.count("*").alias("son") )\
                .filter( col("action_band") =="1" )
        )

        final_df = df_conn.join(df_steer, on=["rowkey"], how="left")\
                            .join(stationary_daily_df, on=["sn", "station_mac"], how="left")
        
        #final_df.write.mode("overwrite").parquet(self.output_path)
        return final_df

    def run(self):
        """Wrapper: run both steps end-to-end."""
        stationary_daily_df = self.compute_stationarity()
        final_df = self.process_hourly(stationary_daily_df)
        return final_df

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="StationConnection Hourly Job")
    parser.add_argument("--date_str", type=str, help="Date string in format YYYYMMDD")
    parser.add_argument("--hour_str", type=str, help="Hour string in format HH")
    return parser.parse_args()

# ============================================================
# Main entry
# ============================================================
if __name__ == "__main__":

    spark = (
        SparkSession.builder
        .appName("StationConnection_bhr_wifi_score_hourly_report_v1")
        .config("spark.ui.port", "24045")
        .getOrCreate()
    )

    # --------------------------------------------------------
    # HDFS paths configuration
    # --------------------------------------------------------
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000"

    station_history_path = f"{hdfs_pa}/sha_data/StationHistory"
    station_connection_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_connection_hourly"

    # Initialize Hadoop FileSystem
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )

    # Prepare current date/hour (with +3 hours offset)
    args = parse_args()
    if args.date_str and args.hour_str:
        date_str = args.date_str
        hour_str = args.hour_str
        print(f"[INFO] Using passed-in date/hour: {date_str} {hour_str}")
    else:
        current_datetime = datetime.now() + timedelta(hours=3)
        date_str = current_datetime.strftime("%Y%m%d")
        hour_str = current_datetime.strftime("%H")
        print(f"[INFO] Auto-generated date/hour: {date_str} {hour_str}")


    # --------------------------------------------------------
    # 1.Delete existing output path (if exists)
    # --------------------------------------------------------
    connection_path_obj = spark._jvm.org.apache.hadoop.fs.Path(station_connection_path)
    if hadoop_fs.exists(connection_path_obj):
        print(f"[INFO] Deleting existing path: {station_connection_path}")
        hadoop_fs.delete(connection_path_obj, True)  # recursive delete
    else:
        print(f"[INFO] Output path does not exist yet: {station_connection_path}")


    # --------------------------------------------------------
    # 2.Wait until the expected input path is available
    # --------------------------------------------------------
    station_history_full_path = f"{station_history_path}/date={date_str}/hour={hour_str}"
    history_path_obj = spark._jvm.org.apache.hadoop.fs.Path(station_history_full_path)

    max_retries = 6           # total wait time = 6 × 10 minutes = 1 hour
    sleep_minutes = 10

    for retry_count in range(max_retries):
        if hadoop_fs.exists(history_path_obj) and spark.read.parquet(station_history_full_path).count() > 4e8:
#        if hadoop_fs.exists(history_path_obj):
            print(f"[OK] Path found: {station_history_full_path}")
            break
        print(f"[{retry_count + 1}/{max_retries}] Path not found, waiting {sleep_minutes} minutes...")
        time.sleep(sleep_minutes * 60)
    else:
        # Executed only if the loop completes without 'break'
        raise FileNotFoundError(
            f"[ERROR] Path not found after {max_retries} attempts: {station_history_full_path}"
        )

    # --------------------------------------------------------
    # 3.Run processing
    # --------------------------------------------------------
    with LogTime() as timer:
        processor = StationConnectionProcessor(
            spark=spark,
            date_str=date_str,
            hour_str=hour_str,
            input_path=station_history_path,
            output_path=station_connection_path,
        )
        station_connection_df = processor.run()
        station_connection_df.write.mode("overwrite").parquet(processor.output_path)
    print("[DONE] StationConnectionProcessor completed successfully.")
