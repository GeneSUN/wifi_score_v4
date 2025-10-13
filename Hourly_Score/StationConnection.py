from datetime import datetime, timedelta, date
from functools import reduce

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

    def __init__(self, spark, input_path, output_path, date_str, hour_str):
        """
        :param spark: active SparkSession
        :param input_path: str, base HDFS path
        :param output_path: str, output HDFS path
        :param date_str: str, e.g. "20250901"
        :param hour_str: str, e.g. "13"
        """
        self.spark = spark
        self.input_path = input_path.rstrip("/")
        self.output_path = output_path.rstrip("/")
        self.date_str = date_str
        self.hour_str = hour_str

    def compute_stationarity(self):
        """Step 1: Compute stationarity (daily level)."""
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
        return stationary_daily_df

    def process_hourly(self, stationary_daily_df):
        """Step 2: Process hourly data and join with stationarity."""
        w = Window.partitionBy("station_mac").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

        exploded_df = (
            self.spark.read.parquet(f"{self.input_path}/date={self.date_str}/hour={self.hour_str}")
            .select("Tplg_Data_model_name", "rowkey", "station_data_connect_data.*")
            .withColumn("sn", F.regexp_extract("rowkey", r"-([A-Z0-9]+)_", 1))
            .withColumn("phy_rate", F.regexp_replace("link_rate", "Mbps", "").cast(IntegerType()))
            .withColumn("tx_phy_rate", F.regexp_replace("tx_link_rate", "Mbps", "").cast(IntegerType()))
            .withColumn("hour", F.date_format(F.from_unixtime(F.col("ts") / 1000), "HH"))
            .withColumn("date", F.date_format(F.from_unixtime(F.col("ts") / 1000), "yyyyMMdd"))
            .select(
                "sn",
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
            .withColumn("son", F.lit(0))
            .withColumn("p50_rssi", F.percentile_approx("rssi", 0.5).over(w).cast("int"))
            .withColumn("p90_rssi", F.percentile_approx("rssi", 0.9).over(w).cast("int"))
            .join(stationary_daily_df, on=["sn", "station_mac"], how="left")
        )

        exploded_df.write.mode("overwrite").parquet(self.output_path)
        return exploded_df

    def run(self):
        """Wrapper: run both steps end-to-end."""
        stationary_daily_df = self.compute_stationarity()
        final_df = self.process_hourly(stationary_daily_df)
        return final_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_bhr_wifi_score_hourly_report_v1')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()

    
    date_str = "20250930"
    hour_str = "15"
    #station_connection_hourly
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    station_history_path = hdfs_pa + f"/sha_data/StationHistory/"

    with LogTime() as timer:
        processor = StationConnectionProcessor(
            spark,
            input_path= hdfs_pa + "/sha_data/StationHistory/",
            output_path= hdfs_pa + "/sha_data//vz-bhr-athena/reports/station_connection_houry/",
            date_str= date_str,
            hour_str= hour_str
        )
        station_connection_df = processor.run()
        station_connection_df = spark.read.parquet(f"{hdfs_pa}/sha_data//vz-bhr-athena/reports/station_connection_houry/")
        