"""
Docstring for wifi_score_hourly

This class calculates a holistic Wi-Fi Quality Score by evaluating three key pillars: 
Speed, Coverage, and Reliability. It uses a data-driven weighting system to ensure 
that the most active devices in a home have the greatest impact on the final score.

Phase 1: Data Consumption Analysis 📉
The script identifies 'Heavy Hitters' by calculating the total bytes (bs + br) 
per station.
- Usage Limit: Filters out records exceeding ~1.8GB/hour to remove anomalies.
- Ranking: Devices within a home (sn) are ranked by total data consumption.

Phase 2: The Three Performance Pillars 🏗️
The script calculates individual scores (1-4) for three dimensions:

1. Speed Score ⚡ (Based on p90 TX PHY Rate):
   -------------------------------------------------------------------------
   | Band | Excellent (4) | Good (3)       | Fair (2)       | Poor (1)     |
   |------|---------------|----------------|----------------|--------------|
   | 2.4G | > 120 Mbps    | 101-120 Mbps   | 80-100 Mbps    | < 80 Mbps    |
   | 5G   | > 400 Mbps    | 301-400 Mbps   | 200-300 Mbps   | < 200 Mbps   |
   -------------------------------------------------------------------------

2. Coverage Score 📡 (Based on p90 RSSI):
   -------------------------------------------------------------------------
   | Band | Excellent (4) | Good (3)       | Fair (2)       | Poor (1)     |
   |------|---------------|----------------|----------------|--------------|
   | 2.4G | >= -55 dBm    | -70 to -56 dBm | -79 to -71 dBm | < -79 dBm    |
   | 5G   | >= -55 dBm    | -70 to -56 dBm | -77 to -71 dBm | < -77 dBm    |
   -------------------------------------------------------------------------

3. Reliability Score 🛡️ (Based on p90 Airtime Utilization):
   -------------------------------------------------------------------------
   | Metric             | Excellent (4) | Good (3) | Fair (2) | Poor (1)    |
   |--------------------|---------------|----------|----------|-------------|
   | Airtime Util (2.4G)| < 40%         | 40-50%   | 50-70%   | > 70%       |
   -------------------------------------------------------------------------

Phase 3: Home-Level Weighting (The 80/20 Rule) ⚖️
To reflect actual user experience, individual station scores are aggregated 
into a weighted home average:
- Top 5 Devices (by consumption): Account for 80% of the home score.
- Remaining Devices: Account for 20% of the home score.

Phase 4: Variation Penalty & Final Calculation 🏆
- Stability Check: If SNR range > 13, a 'Speed Variation' penalty (1 point) 
  is subtracted.
- Final Score: The arithmetic mean of the Speed, Coverage, and Reliability 
  pillars.
- Categorization:
    - > 3.5: Excellent (4)
    - > 2.5: Good (3)
    - > 1.5: Fair (2)
    - <= 1.5: Poor (1)
"""

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
from StationConnection import parse_args, LogTime

class wifi_score_hourly:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    def __init__(self,
                 spark, 
                 date_str,
                 hour_str,
                 source_df,
                 station_history_path,
                 device_groups_path,
                 output_path
                 ):
        self.spark = spark
        self.data_consumption_df = None
        self.date_str = date_str
        self.hour_str = hour_str
        self.source_df = source_df
        self.station_history_path = station_history_path
        self.device_groups_path = device_groups_path
        self.output_path = output_path

    def read_data_consumption(self, date_part, hour_part):
        self.spark.read.parquet(
            f"{self.station_history_path}/date={date_part}/hour={hour_part}"
        )\
         .withColumn("date", F.lit(date_part))\
         .withColumn("hour", F.lit(hour_part))\
         .createOrReplaceTempView('bhrx_stationhistory_version_001')
        
        data_consumption_query = f"""
            SELECT
                regexp_extract(rowkey, '-([^-_]+)_', 1) as sn,
                station_data_connect_data.station_mac as station_mac,
                date,
                hour,
                SUM(CAST(station_data_connect_data.bs as DECIMAL(38, 0)) + 
                    CAST(station_data_connect_data.br as DECIMAL(38, 0))) as total_bytes
            FROM bhrx_stationhistory_version_001
            WHERE date = '{date_part}'
                AND hour = '{hour_part}'
                AND station_data_connect_data IS NOT NULL
            GROUP BY
                regexp_extract(rowkey, '-([^-_]+)_', 1),
                station_data_connect_data.station_mac,
                date,
                hour
            HAVING SUM(CAST(station_data_connect_data.bs AS DECIMAL(38, 0)) +
            CAST(station_data_connect_data.br AS DECIMAL(38, 0))) < 1875000000;
        """

        data_consumption_df = self.spark.sql(data_consumption_query)


        return data_consumption_df

    def calculate_speed_score(self, source_df, data_consumption_df):

        window_spec = Window.partitionBy("sn", "station_mac", "date", "hour")

        station_hourly_data_cached_speed = source_df.withColumn(
                                    "record_count", 
                                    F.count("*").over(window_spec)
                                )

        # -----------------------------
        # 1) pivoted_data (same logic as SQL)
        #   - record_count >= 8
        #   - CAST(tx_phy_rate AS DOUBLE) > 24
        #   - p90 tx_phy_rate for 2.4/5/6
        # -----------------------------
        pivoted_data = (
            station_hourly_data_cached_speed.filter((F.col("record_count") >= 8) & (F.col("tx_phy_rate").cast("double") > 24))
            .groupBy("sn", "station_mac", "date", "hour")
            .agg(
                F.max("model").alias("model"),
                F.max("mobility_status").alias("mobility_status"),
                F.max("station_name").alias("station_name"),

                # APPROX_PERCENTILE(...) -> percentile_approx in Spark
                F.expr(
                    "percentile_approx(CASE WHEN band LIKE '2.4G%' THEN CAST(tx_phy_rate AS DOUBLE) END, 0.90)"
                ).alias("p90_phy_rate_2_4g"),
                F.expr(
                    "percentile_approx(CASE WHEN band LIKE '5G%' THEN CAST(tx_phy_rate AS DOUBLE) END, 0.90)"
                ).alias("p90_phy_rate_5g"),
                F.expr(
                    "percentile_approx(CASE WHEN band LIKE '6G%' THEN CAST(tx_phy_rate AS DOUBLE) END, 0.90)"
                ).alias("p90_phy_rate_6g"),
            )
        )

        # -----------------------------
        # 2) base_scores
        # Change thresholds to match your *previous* script style:
        #   - 2.4G: >=50 ->4, 20-49 ->3, 10-19 ->2, <10 ->1
        #   - 5G  : >=200->4, 100-199->3, 50-99 ->2, <50 ->1
        # Add 6G: (use same as 5G unless you have a different rule)
        # -----------------------------
        base_scores = (
            pivoted_data
            .withColumn(
                "phy_rate_score_2_4g",
                F.when(F.col("p90_phy_rate_2_4g") >= 50, 4)
                .when((F.col("p90_phy_rate_2_4g") >= 20) & (F.col("p90_phy_rate_2_4g") <= 49), 3)
                .when((F.col("p90_phy_rate_2_4g") >= 10) & (F.col("p90_phy_rate_2_4g") <= 19), 2)
                .when(F.col("p90_phy_rate_2_4g") < 10, 1)
            )
            .withColumn(
                "phy_rate_score_5g",
                F.when(F.col("p90_phy_rate_5g") >= 200, 4)
                .when((F.col("p90_phy_rate_5g") >= 100) & (F.col("p90_phy_rate_5g") <= 199), 3)
                .when((F.col("p90_phy_rate_5g") >= 50) & (F.col("p90_phy_rate_5g") <= 99), 2)
                .when(F.col("p90_phy_rate_5g") < 50, 1)
            )
            .withColumn(
                "phy_rate_score_6g",
                F.when(F.col("p90_phy_rate_6g") >= 200, 4)
                .when((F.col("p90_phy_rate_6g") >= 100) & (F.col("p90_phy_rate_6g") <= 199), 3)
                .when((F.col("p90_phy_rate_6g") >= 50) & (F.col("p90_phy_rate_6g") <= 99), 2)
                .when(F.col("p90_phy_rate_6g") < 50, 1)
            )
        )

        # -----------------------------
        # 3) station_base_score_step1_speed
        # Original SQL = average of available (non-null) band scores (2.4 & 5).
        # Now extend to (2.4, 5, 6) with same averaging logic:
        #   sum(scores where not null) / count(non-null scores)
        # -----------------------------
        combined_base_score = (
            base_scores
            .withColumn(
                "station_base_score_step1_speed",
                (
                    F.coalesce(F.col("phy_rate_score_2_4g").cast("double"), F.lit(0.0)) +
                    F.coalesce(F.col("phy_rate_score_5g").cast("double"), F.lit(0.0)) +
                    F.coalesce(F.col("phy_rate_score_6g").cast("double"), F.lit(0.0))
                )
                /
                F.when(
                    (F.col("phy_rate_score_2_4g").isNotNull().cast("int") +
                    F.col("phy_rate_score_5g").isNotNull().cast("int") +
                    F.col("phy_rate_score_6g").isNotNull().cast("int")) == 0,
                    F.lit(None).cast("double")   # mimic NULLIF(...,0) => NULL when denominator=0
                ).otherwise(
                    (F.col("phy_rate_score_2_4g").isNotNull().cast("int") +
                    F.col("phy_rate_score_5g").isNotNull().cast("int") +
                    F.col("phy_rate_score_6g").isNotNull().cast("int")).cast("double")
                )
            )
        )
        
        combined_base_score = combined_base_score.alias("bs").join(
            data_consumption_df.alias("dc"),
            on=['sn', 'station_mac', 'date', 'hour'], 
            how='left_outer'
        ).selectExpr(
            "bs.*",
            "dc.total_bytes"
        )

        window_spec_rank_per_sn = Window.partitionBy("sn", "date", "hour").orderBy(F.desc("total_bytes"))
        stations_with_rank_per_sn = combined_base_score.withColumn("rank_per_sn", F.rank().over(window_spec_rank_per_sn))

        avg_top_5_per_sn_df = stations_with_rank_per_sn.filter(F.col("rank_per_sn") <= 5) \
                                                        .groupBy("sn", "date", "hour") \
                                                        .agg(F.avg("station_base_score_step1_speed").alias("avg_t_per_sn"))
        
        avg_rest_per_sn_df = stations_with_rank_per_sn.filter(F.col("rank_per_sn") > 5) \
                                                    .groupBy("sn", "date", "hour") \
                                                    .agg(F.avg("station_base_score_step1_speed").alias("avg_b_per_sn"))

        temp_df_with_averages = stations_with_rank_per_sn.alias("s").join(
            avg_top_5_per_sn_df.alias("t"),
            on=["sn", "date", "hour"],
            how="left_outer"
        ).join(
            avg_rest_per_sn_df.alias("b"),
            on=["sn", "date", "hour"],
            how="left_outer"
        ).selectExpr(
            "s.*",
            "t.avg_t_per_sn",
            "b.avg_b_per_sn"
        )

        output_df_with_weighted_base = temp_df_with_averages.withColumn(
            "weighted_avg_base_score_speed",
            F.round(
                (F.coalesce(F.col("avg_t_per_sn"), F.lit(0.0)) * 0.80) +
                (F.coalesce(F.col("avg_b_per_sn"), F.lit(0.0)) * 0.20),
                2
            )
        )

        output_df_with_weighted_base = output_df_with_weighted_base.drop("rank_per_sn", "avg_t_per_sn", "avg_b_per_sn")

        # 1. Aggregation Phase (variation_components CTE)
        variation_components_df = (
            station_hourly_data_cached_speed
            .filter(
                (F.col("record_count") >= 8) & 
                (F.col("snr").cast("double").between(0, 95))
            )
            .groupBy("sn", "station_mac", "date", "hour")
            .agg(
                (F.max(F.col("snr").cast("double")) - F.min(F.col("snr").cast("double"))).alias("snr_range")
            )
        )

        # 2. Scoring Phase
        variation_cal = variation_components_df.withColumn(
            "speed_variation_score",
            F.when(F.col("snr_range") > 13, 1).otherwise(0)
        )


        final_speed_score_df = output_df_with_weighted_base.alias("b").join(
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
            "b.p90_phy_rate_2_4g as p90_phy_rate_2_4g_speed",
            "b.p90_phy_rate_5g as p90_phy_rate_5g_speed",  
            "b.p90_phy_rate_6g   as p90_phy_rate_6g_speed",          # ✅ NEW

            "b.phy_rate_score_2_4g as phy_rate_score_2_4g_speed",
            "b.phy_rate_score_5g as phy_rate_score_5g_speed",
            "b.phy_rate_score_6g   as phy_rate_score_6g_speed",      # ✅ NEW

            "v.snr_range AS snr_range_variation",
            "b.station_base_score_step1_speed as individual_station_avg_base_score_speed",
            "b.weighted_avg_base_score_speed as home_weighted_avg_base_score_speed",
            "v.speed_variation_score",
            "(b.weighted_avg_base_score_speed - v.speed_variation_score) AS speed_score_num"
        )

        #station_hourly_data_with_count.unpersist()
        self.spark.catalog.dropTempView('station_hourly_data_cached_speed')
        return final_speed_score_df

    def calculate_coverage_score(self, source_df, data_consumption_df):
        source_df.createOrReplaceTempView('station_hourly_data_coverage')

        station_hourly_data_with_count = self.spark.sql('''
            SELECT
                *,
                COUNT(*) OVER (PARTITION BY sn, station_mac, date, hour) as record_count
            FROM station_hourly_data_coverage
        ''')
        self.spark.catalog.dropTempView('station_hourly_data_coverage')

        #station_hourly_data_with_count.cache()
        station_hourly_data_with_count.createOrReplaceTempView('station_hourly_data_cached_coverage')

        combined_base_score = self.spark.sql('''
            WITH
            pivoted_data AS (
                SELECT
                sn, station_mac, date, hour,
                MAX(model) as model,
                MAX(mobility_status) as mobility_status,
                MAX(station_name) as station_name,
                APPROX_PERCENTILE(CASE WHEN band LIKE '6G%' THEN CAST(rssi AS DOUBLE) END, 0.90) AS p90_rssi_6g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '2.4G%' THEN CAST(rssi AS DOUBLE) END, 0.90) AS p90_rssi_2_4g,
                APPROX_PERCENTILE(CASE WHEN band LIKE '5G%' THEN CAST(rssi AS DOUBLE) END, 0.90) AS p90_rssi_5g
                
                FROM station_hourly_data_cached_coverage
                WHERE record_count >= 8
                GROUP BY sn, station_mac, date, hour
            ),
            base_scores AS (
                SELECT
                *,
                CASE 
                    WHEN p90_rssi_2_4g >= -55 THEN 4 
                    WHEN p90_rssi_2_4g BETWEEN -70 AND -56 THEN 3 
                    WHEN p90_rssi_2_4g BETWEEN -79 AND -71 THEN 2 
                    WHEN p90_rssi_2_4g < -79 THEN 1 
                END AS rssi_score_2_4g,
                CASE 
                    WHEN p90_rssi_5g >= -55 THEN 4 
                    WHEN p90_rssi_5g BETWEEN -70 AND -56 THEN 3 
                    WHEN p90_rssi_5g BETWEEN -77 AND -71 THEN 2 
                    WHEN p90_rssi_5g < -77 THEN 1 
                END AS rssi_score_5g,
                                             
                -- ✅ NEW: 6G coverage thresholds (use same as 5G unless you have a new spec)
                CASE
                    WHEN p90_rssi_6g >= -55 THEN 4
                    WHEN p90_rssi_6g BETWEEN -70 AND -56 THEN 3
                    WHEN p90_rssi_6g BETWEEN -77 AND -71 THEN 2
                    WHEN p90_rssi_6g < -77 THEN 1
                END AS rssi_score_6g
                                             
                                             
                FROM pivoted_data
            )
            SELECT
            *,
            (CASE WHEN rssi_score_2_4g IS NOT NULL THEN rssi_score_2_4g ELSE 0 END +
            CASE WHEN rssi_score_5g IS NOT NULL THEN rssi_score_5g ELSE 0 END) /
            NULLIF((CASE WHEN rssi_score_2_4g IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN rssi_score_5g IS NOT NULL THEN 1 ELSE 0 END), 0) AS station_base_score_step1_coverage
            FROM base_scores
        ''')
        
        combined_base_score = combined_base_score.alias("bs").join(
            data_consumption_df.alias("dc"),
            on=['sn', 'station_mac', 'date', 'hour'], 
            how='left_outer'
        ).selectExpr(
            "bs.*",
            "dc.total_bytes"
        )

        window_spec_rank_per_sn = Window.partitionBy("sn", "date", "hour").orderBy(F.desc("total_bytes"))
        stations_with_rank_per_sn = combined_base_score.withColumn("rank_per_sn", F.rank().over(window_spec_rank_per_sn))

        avg_top_5_per_sn_df = stations_with_rank_per_sn.filter(F.col("rank_per_sn") <= 5) \
                                                        .groupBy("sn", "date", "hour") \
                                                        .agg(F.avg("station_base_score_step1_coverage").alias("avg_t_per_sn"))
        
        avg_rest_per_sn_df = stations_with_rank_per_sn.filter(F.col("rank_per_sn") > 5) \
                                                    .groupBy("sn", "date", "hour") \
                                                    .agg(F.avg("station_base_score_step1_coverage").alias("avg_b_per_sn"))

        temp_df_with_averages = stations_with_rank_per_sn.alias("s").join(
            avg_top_5_per_sn_df.alias("t"),
            on=["sn", "date", "hour"],
            how="left_outer"
        ).join(
            avg_rest_per_sn_df.alias("b"),
            on=["sn", "date", "hour"],
            how="left_outer"
        ).selectExpr(
            "s.*",
            "t.avg_t_per_sn",
            "b.avg_b_per_sn"
        )

        output_df_with_weighted_base = temp_df_with_averages.withColumn(
            "weighted_avg_base_score_coverage",
            F.round(
                (F.coalesce(F.col("avg_t_per_sn"), F.lit(0.0)) * 0.80) +
                (F.coalesce(F.col("avg_b_per_sn"), F.lit(0.0)) * 0.20),
                2
            )
        )

        output_df_with_weighted_base = output_df_with_weighted_base.drop("rank_per_sn", "avg_t_per_sn", "avg_b_per_sn")

        variation_cal = self.spark.sql('''
            WITH
            variation_components AS (
                SELECT
                sn, station_mac, date, hour
                FROM station_hourly_data_cached_coverage
                WHERE record_count >= 8
                GROUP BY sn, station_mac, date, hour
            )
            SELECT
            sn, station_mac, date, hour,
            0 AS rssi_range,
            0 AS coverage_variation_score
            FROM variation_components
        ''')

        final_coverage_score_df = output_df_with_weighted_base.alias("b").join(
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
            "b.p90_rssi_2_4g",
            "b.p90_rssi_5g",
            "b.p90_rssi_6g",              # ✅ NEW

            "b.rssi_score_2_4g",
            "b.rssi_score_5g",
            "b.rssi_score_6g",            # ✅ NEW

            "v.rssi_range AS rssi_range_variation", 
            "b.station_base_score_step1_coverage as individual_station_avg_base_score_coverage",
            "b.weighted_avg_base_score_coverage as home_weighted_avg_base_score_coverage",
            "v.coverage_variation_score", 
            "(b.weighted_avg_base_score_coverage - v.coverage_variation_score) AS coverage_score_num"
        )

        #station_hourly_data_with_count.unpersist()
        self.spark.catalog.dropTempView('station_hourly_data_cached_coverage')
        return final_coverage_score_df

    def calculate_reliability_score(self, date_part, hour_part):

        self.spark.read.parquet(
            f"{self.device_groups_path}/date={date_part}/hour={hour_part}"
        )\
            .withColumn("date", F.lit(date_part))\
            .withColumn("hour", F.lit(hour_part))\
            .createOrReplaceTempView('bhrx_devicegroups_version_001')


        reliability_query = f"""
                                SELECT
                                    t1.date,  
                                    t1.hour,  
                                    regexp_extract(t1.rowkey, '-([^-_]+)', 1) as sn,
                                    APPROX_PERCENTILE(CAST(t.radio_info._2_4g.airtime_util AS INT), 0.90) AS p90_airtime_util_2_4g
                                FROM
                                    bhrx_devicegroups_version_001 AS t1  
                                LATERAL VIEW EXPLODE(t1.group_diag_history_radio_wifi_info) t AS radio_info  
                                WHERE
                                    t1.group_diag_history_radio_wifi_info IS NOT NULL
                                    AND t.radio_info._2_4g.enable = '1'
                                    AND t.radio_info._2_4g.airtime_util IS NOT NULL
                                    AND t.radio_info._2_4g.airtime_util <> ''
                                    AND CAST(t.radio_info._2_4g.airtime_util AS INT) BETWEEN 0 AND 100
                                GROUP BY
                                    t1.date, t1.hour, regexp_extract(t1.rowkey, '-([^-_]+)', 1)
                            """

        reliability_df = self.spark.sql(reliability_query)

        reliability_score_df = reliability_df.withColumn(
            "reliability_score_num",
            F.when(F.col("p90_airtime_util_2_4g") < 40, 4)
            .when(F.col("p90_airtime_util_2_4g").between(40, 50), 3)
            .when(F.col("p90_airtime_util_2_4g").between(50, 70), 2)
            .otherwise(1) # > 70
        )
        return reliability_score_df.select("sn", "date", "hour", "p90_airtime_util_2_4g", "reliability_score_num")

    def run(self):
        self.data_consumption_df = self.read_data_consumption(self.date_str, str(self.hour_str).zfill(2))
        self.coverage_score_df = self.calculate_coverage_score(self.source_df, self.data_consumption_df)
        self.speed_score_df = self.calculate_speed_score(self.source_df, self.data_consumption_df)
        self.reliability_score_df = self.calculate_reliability_score(self.date_str, str(self.hour_str).zfill(2))

        wifi_score_df = self.speed_score_df.alias("s").join(
                self.coverage_score_df.alias("c"),
                on=['sn', 'station_mac', 'date', 'hour', 'model', 'station_name'],
                how='inner'
            ).join(
                self.reliability_score_df.alias("r"),
                on=['sn', 'date', 'hour'],
                how='left_outer'
            ).selectExpr(
                "s.sn",
                "s.station_mac",
                "s.station_name",
                "s.model",
                "s.mobility_status",
                "s.date",
                "s.hour",
                "s.p90_phy_rate_2_4g_speed",
                "s.p90_phy_rate_5g_speed",
                "s.p90_phy_rate_6g_speed",              # ✅ NEW

                "s.phy_rate_score_2_4g_speed",
                "s.phy_rate_score_5g_speed",
                "s.phy_rate_score_6g_speed",            # ✅ NEW

                "s.snr_range_variation", 
                "s.home_weighted_avg_base_score_speed",
                "s.speed_variation_score",
                "s.speed_score_num",
                "c.p90_rssi_2_4g",
                "c.p90_rssi_5g",
                "c.p90_rssi_6g",                         # ✅ NEW

                "c.rssi_score_2_4g",
                "c.rssi_score_5g",
                "c.rssi_score_6g",                       # ✅ NEW

                "c.rssi_range_variation",
                "c.home_weighted_avg_base_score_coverage",
                "c.coverage_variation_score",
                "c.coverage_score_num",
                "r.p90_airtime_util_2_4g",
                "r.reliability_score_num",
                """
                ROUND(
                    (s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                    NULLIF(
                        (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                        CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                    ), 2
                ) AS wifi_score_num_unrounded
                """,
                """
                CASE
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 3.5 THEN 4
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 2.5 THEN 3
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 1.5 THEN 2
                    ELSE 1
                END AS wifi_score_num
                """,
                """
                CASE
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 3.5 THEN 'Excellent'
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 2.5 THEN 'Good'
                    WHEN ((s.speed_score_num + c.coverage_score_num + COALESCE(r.reliability_score_num, 0)) /
                        NULLIF(
                            (CASE WHEN s.speed_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN c.coverage_score_num IS NOT NULL THEN 1 ELSE 0 END +
                            CASE WHEN r.reliability_score_num IS NOT NULL THEN 1 ELSE 0 END), 0
                        )) > 1.5 THEN 'Fair'
                    ELSE 'Poor'
                END AS wifi_score_category
                """
            )

        fraction = 0.01  # 1% sample
        sample_count = wifi_score_df.sample(fraction=fraction, seed=42).count()
        estimated_total = sample_count / fraction

        if estimated_total > 1_000_000:
            print(f"Estimated row count: {estimated_total:,.0f} — proceeding with write")
            wifi_score_df.write.mode("overwrite").parquet(
                f"{self.output_path}/date={self.date_str}/hour={self.hour_str}"
            )
        else:
            sys.path.append('/usr/apps/vmas/script/ZS') 
            from MailSender import MailSender
            mail_sender = MailSender()
            mail_sender.send(text = f"Estimated row count{date_str} {hour_str}: {estimated_total:,.0f} — below threshold, skipping write", subject="wifiScore_ZheS failed")
            print(f"Estimated row count: {estimated_total:,.0f} — below threshold, skipping write")

        #wifi_score_df.write.mode("overwrite").parquet(f"{self.output_path}/date={self.date_str}/hour={self.hour_str}")
        #return wifi_score_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_bhr_wifi_score_hourly_report_v1')\
                        .config("spark.ui.port","24045")\
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
    wifi_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/wifi_score_hourly"


    # --------------------------------------------------------
    # Run WiFi Score Hourly Processor
    # --------------------------------------------------------
    with LogTime() as timer:
        
        station_connection_df = spark.read.parquet(station_connection_path)

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



