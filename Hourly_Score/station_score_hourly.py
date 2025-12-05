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

sys.path.append('/usr/apps/vmas/script/ZS/HourlyScore') 
from StationConnection import parse_args, LogTime, StationConnectionProcessor

class station_score_hourly:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    def __init__(self,
                 spark,
                 date_str,
                 hour_str,
                 source_df,
                 output_path
                 ):
        self.spark = spark
        self.date_str = date_str
        self.hour_str = hour_str
        self.source_df = source_df

        self.output_path = output_path

    def run(self, source_df = None):
        if source_df is not None:
            self.source_df = source_df

        self.source_df.createOrReplaceTempView('station_hourly_data')
        station_hourly_data_with_count = self.spark.sql('''
            SELECT
                *,
                COUNT(*) OVER (PARTITION BY sn, station_mac, date, hour) as record_count
            FROM station_hourly_data
        ''')
        station_hourly_data_with_count.createOrReplaceTempView('station_hourly_data_cached')

        combined_base_score = self.spark.sql('''
            WITH
            pivoted_data AS (
                SELECT
                sn, station_mac, date, hour,
                MAX(model) as model,
                MAX(mobility_status) as mobility_status,
                MAX(station_name) as station_name,
                APPROX_PERCENTILE(CASE WHEN band LIKE '2.4G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.90) AS p90_rssi_2_4g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '5G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.90) AS p90_rssi_5g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '2.4G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.50) AS p50_rssi_2_4g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '5G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.50) AS p50_rssi_5g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '2.4G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.95) AS p95_rssi_2_4g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '5G%' THEN CAST(p90_rssi AS DOUBLE) END, 0.95) AS p95_rssi_5g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '2.4G%' THEN CAST(phy_rate AS DOUBLE) END, 0.9) AS p90_phy_rate_2_4g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '5G%' THEN CAST(phy_rate AS DOUBLE) END, 0.9) AS p90_phy_rate_5g
                FROM station_hourly_data_cached
                WHERE record_count >= 9
                GROUP BY sn, station_mac, date, hour
            ),
            base_scores AS (
                SELECT
                *,
                CASE WHEN p90_rssi_2_4g >= -65 THEN 4 WHEN p90_rssi_2_4g BETWEEN -75 AND -66 THEN 3 WHEN p90_rssi_2_4g BETWEEN -85 AND -76 THEN 2 WHEN p90_rssi_2_4g < -85 THEN 1 END AS rssi_score_2_4g,
                CASE WHEN p90_rssi_5g >= -68 THEN 4 WHEN p90_rssi_5g BETWEEN -78 AND -69 THEN 3 WHEN p90_rssi_5g BETWEEN -88 AND -79 THEN 2 WHEN p90_rssi_5g < -88 THEN 1 END AS rssi_score_5g,
                CASE WHEN p90_phy_rate_2_4g >= 50 THEN 4 WHEN p90_phy_rate_2_4g BETWEEN 20 AND 49 THEN 3 WHEN p90_phy_rate_2_4g BETWEEN 10 AND 19 THEN 2 WHEN p90_phy_rate_2_4g < 10 THEN 1 END AS phy_rate_score_2_4g,
                CASE WHEN p90_phy_rate_5g >= 200 THEN 4 WHEN p90_phy_rate_5g BETWEEN 100 AND 199 THEN 3 WHEN p90_phy_rate_5g BETWEEN 50 AND 99 THEN 2 WHEN p90_phy_rate_5g < 50 THEN 1 END AS phy_rate_score_5g
                FROM pivoted_data
            )
            SELECT
            *,
            CASE WHEN rssi_score_2_4g IS NOT NULL AND rssi_score_5g IS NOT NULL THEN LEAST(rssi_score_2_4g, rssi_score_5g) WHEN rssi_score_2_4g IS NOT NULL THEN rssi_score_2_4g ELSE rssi_score_5g END AS final_rssi_score,
            CASE WHEN phy_rate_score_2_4g IS NOT NULL AND phy_rate_score_5g IS NOT NULL THEN LEAST(phy_rate_score_2_4g, phy_rate_score_5g) WHEN phy_rate_score_2_4g IS NOT NULL THEN phy_rate_score_2_4g ELSE phy_rate_score_5g END AS final_phy_rate_score
            FROM base_scores
        ''')

        variation_cal = self.spark.sql('''
            WITH
            variation_components AS (
                SELECT
                sn, station_mac, date, hour,
                MAX(CAST(snr AS DOUBLE)) - MIN(CAST(snr AS DOUBLE)) as snr_range,
                SUM(CASE WHEN son >= 1 THEN 1 ELSE 0 END) as son_count,
                MAX(CASE WHEN band LIKE '2.4G%' THEN CAST(phy_rate AS DOUBLE) END) - MIN(CASE WHEN band LIKE '2.4G%' THEN CAST(phy_rate AS DOUBLE) END) as phy_rate_range_2_4g,
                MAX(CASE WHEN band LIKE '5G%' THEN CAST(phy_rate AS DOUBLE) END) - MIN(CASE WHEN band LIKE '5G%' THEN CAST(phy_rate AS DOUBLE) END) as phy_rate_range_5g
                FROM station_hourly_data_cached
                WHERE record_count >= 9
                GROUP BY sn, station_mac, date, hour
            )
            SELECT
            sn, station_mac, date, hour,
            snr_range,
            son_count,
            phy_rate_range_2_4g,
            phy_rate_range_5g,
            CASE
                WHEN snr_range > 13
                OR son_count > 6
                OR phy_rate_range_2_4g > 30
                OR phy_rate_range_5g > 300
                THEN 1
                ELSE 0
            END AS variation_score
            FROM variation_components
        ''')


        output_df = combined_base_score.alias("b").join(
            variation_cal.alias("v"),
            on=['sn', 'station_mac', 'date', 'hour'],
            how='inner'
        ).selectExpr(
            "b.sn",
            "b.station_mac",
            "b.station_name",
            "b.model",
            "b.mobility_status",
            "b.date",
            "b.hour",
            "b.p90_rssi_2_4g AS p90_rssi_2_4g_base",
            "b.p90_rssi_5g AS p90_rssi_5g_base",
            "b.p50_rssi_2_4g AS p50_rssi_2_4g_base",
            "b.p50_rssi_5g AS p50_rssi_5g_base",
            "b.p95_rssi_2_4g AS p95_rssi_2_4g_base",
            "b.p95_rssi_5g AS p95_rssi_5g_base",
            "b.p90_phy_rate_2_4g AS p90_phy_rate_2_4g_base",
            "b.p90_phy_rate_5g AS p90_phy_rate_5g_base",
            "b.rssi_score_2_4g AS rssi_score_2_4g_base",
            "b.rssi_score_5g AS rssi_score_5g_base",
            "b.phy_rate_score_2_4g AS phy_rate_score_2_4g_base",
            "b.phy_rate_score_5g AS phy_rate_score_5g_base",
            "v.snr_range AS snr_range_variation",
            "v.son_count AS son_count_variation",
            "v.phy_rate_range_2_4g AS phy_rate_range_2_4g_variation",
            "v.phy_rate_range_5g AS phy_rate_range_5g_variation",
            "LEAST(b.final_rssi_score, b.final_phy_rate_score) AS base_score",
            "v.variation_score",
            """CASE
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score >= 4 THEN 'Excellent'
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score = 3 THEN 'Good'
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score = 2 THEN 'Fair'
            ELSE 'Poor'
            END AS station_score""",
            """CASE
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score >= 4 THEN 4
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score = 3 THEN 3
            WHEN LEAST(b.final_rssi_score, b.final_phy_rate_score) - v.variation_score = 2 THEN 2
            ELSE 1
            END AS station_score_num"""
        )


        output_df.write.mode("overwrite").parquet(f"{self.output_path}/date={self.date_str}/hour={self.hour_str}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName('station_score_hourly')\
                        .config("spark.ui.port","24046")\
                        .getOrCreate()


    # --------------------------------------------------------
    # Parse Input Arguments or Auto-Generate Date/Hour
    # --------------------------------------------------------
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
    # Define HDFS Paths
    # --------------------------------------------------------
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000"

    station_history_path = f"{hdfs_pa}/sha_data/StationHistory"
    device_groups_path = f"{hdfs_pa}/sha_data/DeviceGroups"
    station_connection_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_connection_hourly"
    station_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_score_hourly"

    
    # --------------------------------------------------------
    # Run WiFi Score Hourly Processor
    # --------------------------------------------------------

    with LogTime() as timer:
        station_connection_df = spark.read.parquet(station_connection_path)

        ins = station_score_hourly(
                                spark,
                                date_str,
                                hour_str,
                                station_connection_df,
                                output_path = station_score_output_path
                                )

        ins.run()