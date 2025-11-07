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

from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
import re

class StationHistoryReader:
    def __init__(self, spark, base_path="/sha_data/StationHistory/", lookback_minutes=60):
        self.spark = spark
        self.base_path = base_path
        self.lookback_minutes = lookback_minutes   # ✅ configurable window
        self.fs = self._get_fs()

    def _get_fs(self):
        sc = self.spark.sparkContext
        java_import(sc._gateway.jvm, "org.apache.hadoop.fs.FileSystem")
        java_import(sc._gateway.jvm, "org.apache.hadoop.fs.Path")
        return sc._gateway.jvm.FileSystem.get(sc._gateway.jvm.org.apache.hadoop.conf.Configuration())

    def _list_dirs(self, path):
        """Return list of directories under path."""
        p = self._jPath(path)
        if not self.fs.exists(p):
            return []
        statuses = self.fs.listStatus(p)
        return [st.getPath().getName() for st in statuses if st.isDirectory()]

    def _list_files(self, path):
        """Return list of files under path."""
        p = self._jPath(path)
        if not self.fs.exists(p):
            return []
        statuses = self.fs.listStatus(p)
        return [st.getPath().getName() for st in statuses if st.isFile()]

    def _jPath(self, p):
        return self.spark._jvm.org.apache.hadoop.fs.Path(p)

    def get_latest_date(self):
        date_paths = self._list_dirs(self.base_path)
        dates = [d.replace('date=', '') for d in date_paths if d.startswith("date=")]

        from builtins import max as py_max
        return py_max(dates, key=lambda x: int(x)) if dates else None

    def get_latest_hour(self, date):
        hour_paths = self._list_dirs(f"{self.base_path}/date={date}")
        hours = [int(h.replace("hour=", "")) for h in hour_paths if h.startswith("hour=")]
        
        from builtins import max as py_max
        return py_max(hours, key=lambda x: int(x)) if hours else None

    def get_latest_timestamp(self, date, hour):
        hour_path = f"{self.base_path}/date={date}/hour={hour:02d}"
        files = self._list_files(hour_path)

        # extract timestamp from filenames
        timestamps = []
        for f in files:
            m = re.search(r'_(\d+)\.parquet$', f)
            if m:
                timestamps.append(int(m.group(1)))
        from builtins import max as py_max
        return sorted(timestamps,reverse=True)[0]

    # ✅ New method
    def get_recent_files(self):
        date = self.get_latest_date()
        hour = self.get_latest_hour(date)
        latest_ts = self.get_latest_timestamp(date, hour)
    
        if latest_ts is None:
            return []
    
        cutoff_ms = self.lookback_minutes * 60 * 1000
        threshold_ts = latest_ts - cutoff_ms
    
        result_files = []
    
        # ---- 1. Check latest hour ----
        curr_path = f"{self.base_path}/date={date}/hour={hour:02d}"
        result_files.extend(self._filter_files_by_ts(curr_path, threshold_ts, latest_ts))
    
        # ---- 2. Check previous hour or previous date:hour=23 ----
        if hour > 0:
            prev_hour = hour - 1
            prev_path = f"{self.base_path}/date={date}/hour={prev_hour:02d}"
            result_files.extend(self._filter_files_by_ts(prev_path, threshold_ts, latest_ts))
        else:
            # hour == 0 → go to previous date at hour=23
            from datetime import datetime, timedelta
            dt = datetime.strptime(date, "%Y%m%d")
            prev_date_str = (dt - timedelta(days=1)).strftime("%Y%m%d")
            prev_path = f"{self.base_path}/date={prev_date_str}/hour=23"
            result_files.extend(self._filter_files_by_ts(prev_path, threshold_ts, latest_ts))
    
        # ✅ Return list and DataFrame directly
        df = self.spark.read.parquet(*result_files)
        return result_files, df


    # ✅ Helper to filter files within timestamp range
    def _filter_files_by_ts(self, hour_path, threshold_ts, latest_ts):
        files = self._list_files(hour_path)
        selected_files = []

        for fname in files:
            m = re.search(r'_(\d+)\.parquet$', fname)
            if not m:
                continue
            ts = int(m.group(1))
            if threshold_ts <= ts <= latest_ts:
                selected_files.append(f"{hour_path}/{fname}")

        return selected_files

    def info(self):
        date = self.get_latest_date()
        hour = self.get_latest_hour(date)
        ts = self.get_latest_timestamp(date, hour)
        print(f"Latest date: {date}")
        print(f"Latest hour: {hour}")
        print(f"Latest timestamp: {ts}")


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
            station_history_df: DataFrame,
            date_str: str,
            input_path: str,
            output_path: Optional[str] = None,
        ):
        self.spark = spark
        self.station_history_df = station_history_df
        self.date_str = date_str
        self.input_path = input_path.rstrip("/")
        #self.output_path = output_path.rstrip("/") if output_path else None
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
            self.station_history_df
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
            self.station_history_df
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


# ============================================================
# Main entry
# ============================================================
if __name__ == "__main__":

    # --------------------------------------------------------
    # 1. Spark session initialization
    # --------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("StationConnection_bhr_wifi_score_hourly_report_v1")
        .config("spark.ui.port", "24045")
        .getOrCreate()
    )
    
    # --------------------------------------------------------
    # 2. Core configuration
    # --------------------------------------------------------
    date_str = datetime.now().strftime("%Y%m%d")

    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000"

    station_history_path = f"{hdfs_pa}/sha_data/StationHistory"
    station_connection_output = (
        f"{hdfs_pa}/sha_data/hourlyScore_rollingWindow/"
        f"station_connection_rollingWindow"
    )

    # --------------------------------------------------------
    # 3. Load station history data (rolling window)
    # --------------------------------------------------------
    reader = StationHistoryReader(
        spark=spark,
        base_path=station_history_path,    # ✅ use full HDFS path directly
        lookback_minutes=60
    )

    recent_files, station_history_df = reader.get_recent_files()

    # --------------------------------------------------------
    # 4. Run processing
    # --------------------------------------------------------
    with LogTime() as timer:
        processor = StationConnectionProcessor(
            spark=spark,
            station_history_df=station_history_df,
            date_str=date_str,
            input_path=station_history_path
        )

        station_connection_df = processor.run()
        station_connection_df.show(5, truncate=False)

        # Optional write
        station_connection_df.write.mode("overwrite").parquet(station_connection_output)

    print("[DONE] StationConnectionProcessor completed successfully.")
