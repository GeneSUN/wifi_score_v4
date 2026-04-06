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
    hdfs_pa = "hdfs://njbbepapa1.nss.vzwnet.com:9000"

    def __init__(self, spark, date_str, hour_str, source_df, output_path):
        self.spark = spark
        self.date_str = date_str
        self.hour_str = hour_str
        self.source_df = source_df
        self.output_path = output_path

    def run(self, source_df=None):
        if source_df is not None:
            self.source_df = source_df

        df = self.source_df

        # ============================================================
        # [NEW - Req 1] Remove rows where PhyRate = 0 AND RSSI = -100
        #               These are sentinel/invalid "no signal" records.
        # ============================================================
        df = df.filter(
            ~(
                (F.col("phy_rate").cast("double") == F.lit(0.0))
                & (F.col("rssi").cast("double") == F.lit(0.0))
                & (F.col("p90_rssi").cast("double") == F.lit(-100.0))
            )
        )
        # ============================================================
        # [NEW - Req 2] Remove rows with no data (null in all key
        #               metric columns used for scoring).
        # ============================================================
        df = df.filter(
            F.col("p90_rssi").isNotNull()
            | F.col("phy_rate").isNotNull()
            | F.col("snr").isNotNull()
        )
        # ============================================================
        # [NEW - Req 3] Remove Ethernet devices — band = 'ETH' (or
        #               similar). Adjust the pattern if your data uses
        #               a different label (e.g. 'Ethernet', 'eth').
        # ============================================================
        df = df.filter(
            ~F.col("band").isin("ETH", "Ether", "ethernet")
            & F.col("band").isNotNull()
        )
        
        # ------------------------------------------------------------
        # 1) record_count = COUNT(*) OVER (PARTITION BY sn, station_mac, date, hour)
        # ------------------------------------------------------------
        w = Window.partitionBy("sn", "station_mac", "date", "hour")
        df_wc = df.withColumn("record_count", F.count(F.lit(1)).over(w))

        # SQL used WHERE record_count >= 9 for both branches
        df_filt = df_wc.filter(F.col("record_count") >= F.lit(9))

        # ------------------------------------------------------------
        # 2) pivoted_data: group + approx percentiles + max fields
        # ------------------------------------------------------------
        pivoted = (
            df_filt.groupBy("sn", "station_mac", "date", "hour")
            .agg(
                F.max("model").alias("model"),
                F.max("mobility_status").alias("mobility_status"),
                F.max("station_name").alias("station_name"),
                F.max(F.col("mode").cast("integer")).alias("mode"),

                # --- RSSI Percentiles ---
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"), F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"), F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_6g"), # Added 6G
                
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"), F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"), F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_6g"), # Added 6G
                
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"), F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"), F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_6g"), # Added 6G
                
                # --- PHY Rate Percentiles ---
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"), F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"), F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_6g")  # Added 6G

            )
        )

        # ------------------------------------------------------------
        # 3) base_scores + final_* scores (match your SQL CASE logic)
        # ------------------------------------------------------------


        base_scores = (
            pivoted
            # --- RSSI Scores ---
            .withColumn(
                "rssi_score_2_4g",
                F.when(F.col("p90_rssi_2_4g") >= -65, 4)
                .when(F.col("p90_rssi_2_4g").between(-75, -66), 3)
                .when(F.col("p90_rssi_2_4g").between(-85, -76), 2)
                .when(F.col("p90_rssi_2_4g") < -85, 1),
            )
            .withColumn(
                "rssi_score_5g",
                F.when(F.col("p90_rssi_5g") >= -68, 4)
                .when(F.col("p90_rssi_5g").between(-78, -69), 3)
                .when(F.col("p90_rssi_5g").between(-88, -79), 2)
                .when(F.col("p90_rssi_5g") < -88, 1),
            )
            .withColumn(
                "rssi_score_6g", # Added 6G RSSI logic
                F.when(F.col("p90_rssi_6g") >= -62, 4)
                .when(F.col("p90_rssi_6g").between(-72, -63), 3)
                .when(F.col("p90_rssi_6g").between(-82, -73), 2)
                .when(F.col("p90_rssi_6g") < -82, 1),
            )
            # --- PHY Rate Scores (TABLE 1 override, then TABLE 2 fallback) ---
            .withColumn(
                "phy_rate_score_2_4g",
                F.when(
                    ((F.col("mode") == 0) & (F.col("p90_phy_rate_2_4g") <= 2))
                    | (F.col("mode").between(1, 2) & (F.col("p90_phy_rate_2_4g") <= 11))
                    | ((F.col("mode") == 3) & (F.col("p90_phy_rate_2_4g") <= 24))
                    | (F.col("mode").between(4, 7) & (F.col("p90_phy_rate_2_4g") <= 12))
                    | (F.col("mode").between(8, 10) & (F.col("p90_phy_rate_2_4g") <= 6))
                    | (F.col("mode").between(11, 21) & (F.col("p90_phy_rate_2_4g") <= 6))
                    | (F.col("mode").between(22, 29) & (F.col("p90_phy_rate_2_4g") <= 6)),
                    4,
                )
                .when(F.col("p90_phy_rate_2_4g") > 144, 4)
                .when(F.col("p90_phy_rate_2_4g").between(72, 144), 3)
                .when(F.col("p90_phy_rate_2_4g").between(12, 71), 2)
                .when(F.col("p90_phy_rate_2_4g") < 12, 1),
            )
            .withColumn(
                "phy_rate_score_5g",
                F.when(
                    ((F.col("mode") == 0) & (F.col("p90_phy_rate_5g") <= 2))
                    | (F.col("mode").between(1, 2) & (F.col("p90_phy_rate_5g") <= 11))
                    | ((F.col("mode") == 3) & (F.col("p90_phy_rate_5g") <= 24))
                    | (F.col("mode").between(4, 7) & (F.col("p90_phy_rate_5g") <= 12))
                    | (F.col("mode").between(8, 10) & (F.col("p90_phy_rate_5g") <= 6))
                    | (F.col("mode").between(11, 21) & (F.col("p90_phy_rate_5g") <= 6))
                    | (F.col("mode").between(22, 29) & (F.col("p90_phy_rate_5g") <= 6)),
                    4,
                )
                .when(F.col("p90_phy_rate_5g") > 600, 4)
                .when(F.col("p90_phy_rate_5g").between(300, 600), 3)
                .when(F.col("p90_phy_rate_5g").between(86, 299), 2)
                .when(F.col("p90_phy_rate_5g") < 86, 1),
            )
            .withColumn(
                "phy_rate_score_6g",
                F.when(
                    ((F.col("mode") == 0) & (F.col("p90_phy_rate_6g") <= 2))
                    | (F.col("mode").between(1, 2) & (F.col("p90_phy_rate_6g") <= 11))
                    | ((F.col("mode") == 3) & (F.col("p90_phy_rate_6g") <= 24))
                    | (F.col("mode").between(4, 7) & (F.col("p90_phy_rate_6g") <= 12))
                    | (F.col("mode").between(8, 10) & (F.col("p90_phy_rate_6g") <= 6))
                    | (F.col("mode").between(11, 21) & (F.col("p90_phy_rate_6g") <= 6))
                    | (F.col("mode").between(22, 29) & (F.col("p90_phy_rate_6g") <= 6)),
                    4,
                )
                .when(F.col("p90_phy_rate_6g") > 1200, 4)
                .when(F.col("p90_phy_rate_6g").between(600, 1200), 3)
                .when(F.col("p90_phy_rate_6g").between(286, 599), 2)
                .when(F.col("p90_phy_rate_6g") < 286, 1),
            )
        )

        """
                    .withColumn(
                "phy_rate_score_2_4g",
                F.when(F.col("p90_phy_rate_2_4g") >= 50, 4)
                .when(F.col("p90_phy_rate_2_4g").between(20, 49), 3)
                .when(F.col("p90_phy_rate_2_4g").between(10, 19), 2)
                .when(F.col("p90_phy_rate_2_4g") < 10, 1),
            )
        """

        BIG = 10**9  # bigger than any possible score

        combined_base_score = (
            base_scores
            .withColumn(
                "final_rssi_score",
                F.least(
                    F.coalesce(F.col("rssi_score_2_4g"), F.lit(BIG)),
                    F.coalesce(F.col("rssi_score_5g"),   F.lit(BIG)),
                    F.coalesce(F.col("rssi_score_6g"),   F.lit(BIG)),
                )
            )
            .withColumn(
                "final_phy_rate_score",
                F.least(
                    F.coalesce(F.col("phy_rate_score_2_4g"), F.lit(BIG)),
                    F.coalesce(F.col("phy_rate_score_5g"),   F.lit(BIG)),
                    F.coalesce(F.col("phy_rate_score_6g"),   F.lit(BIG)),
                )
            )
            # if all three were NULL, you probably want NULL (not BIG)
            .withColumn(
                "final_rssi_score",
                F.when(
                    F.col("rssi_score_2_4g").isNull() & F.col("rssi_score_5g").isNull() & F.col("rssi_score_6g").isNull(),
                    F.lit(None)
                ).otherwise(F.col("final_rssi_score"))
            )
            .withColumn(
                "final_phy_rate_score",
                F.when(
                    F.col("phy_rate_score_2_4g").isNull() & F.col("phy_rate_score_5g").isNull() & F.col("phy_rate_score_6g").isNull(),
                    F.lit(None)
                ).otherwise(F.col("final_phy_rate_score"))
            )
        )
        # ============================================================
        # [NEW - Req 2 (post-aggregation guard)] Drop any grouped rows
        #        where BOTH final scores are still NULL after pivoting.
        #        This catches devices that had rows but all key metrics
        #        were null across every band after aggregation.
        # ============================================================
        combined_base_score = combined_base_score.filter(
            F.col("final_rssi_score").isNotNull()
            | F.col("final_phy_rate_score").isNotNull()
        )


        # ------------------------------------------------------------
        # 4) variation_cal (no helper; directly use col.like)
        # ------------------------------------------------------------
        variation_components = (
            df_filt.groupBy("sn", "station_mac", "date", "hour")
            .agg(
                (F.max(F.col("snr").cast("double")) - F.min(F.col("snr").cast("double"))).alias("snr_range"),
                F.sum(F.when(F.col("son") >= F.lit(1), F.lit(1)).otherwise(F.lit(0))).alias("son_count"),
                (
                    F.max(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")))
                    - F.min(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")))
                ).alias("phy_rate_range_2_4g"),
                (
                    F.max(F.when(F.col("band").like("5G%"), F.col("phy_rate").cast("double")))
                    - F.min(F.when(F.col("band").like("5G%"), F.col("phy_rate").cast("double")))
                ).alias("phy_rate_range_5g"),
            )
        )

        variation_cal = variation_components.withColumn(
            "variation_score",
            F.when(
                (F.col("snr_range") > F.lit(13))
                | (F.col("son_count") > F.lit(6))
                | (F.col("phy_rate_range_2_4g") > F.lit(30))
                | (F.col("phy_rate_range_5g") > F.lit(300)),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )

        # ------------------------------------------------------------
        # 5) Join + final output columns (EXACT output/logic)
        # ------------------------------------------------------------
        joined = (
            combined_base_score.alias("b")
            .join(variation_cal.alias("v"), on=["sn", "station_mac", "date", "hour"], how="inner")
            .withColumn("base_score", F.least(F.col("b.final_rssi_score"), F.col("b.final_phy_rate_score")))
        )

        score_val = F.col("base_score") - F.col("v.variation_score")

        output_df = joined.select(
            F.col("b.sn").alias("sn"),
            F.col("b.station_mac").alias("station_mac"),
            F.col("b.station_name").alias("station_name"),
            F.col("b.model").alias("model"),
            F.col("b.mobility_status").alias("mobility_status"),
            F.col("b.date").alias("date"),
            F.col("b.hour").alias("hour"),
            F.col("b.p90_rssi_2_4g").alias("p90_rssi_2_4g_base"),
            F.col("b.p90_rssi_5g").alias("p90_rssi_5g_base"),
            F.col("b.p90_rssi_6g").alias("p90_rssi_6g_base"),

            F.col("b.p50_rssi_2_4g").alias("p50_rssi_2_4g_base"),
            F.col("b.p50_rssi_5g").alias("p50_rssi_5g_base"),
            F.col("b.p50_rssi_6g").alias("p50_rssi_6g_base"),

            F.col("b.p95_rssi_2_4g").alias("p95_rssi_2_4g_base"),
            F.col("b.p95_rssi_5g").alias("p95_rssi_5g_base"),
            F.col("b.p95_rssi_6g").alias("p95_rssi_6g_base"),

            F.col("b.p90_phy_rate_2_4g").alias("p90_phy_rate_2_4g_base"),
            F.col("b.p90_phy_rate_5g").alias("p90_phy_rate_5g_base"),
            F.col("b.p90_phy_rate_6g").alias("p90_phy_rate_6g_base"),

            F.col("b.rssi_score_2_4g").alias("rssi_score_2_4g_base"),
            F.col("b.rssi_score_5g").alias("rssi_score_5g_base"),
            F.col("b.rssi_score_6g").alias("rssi_score_6g_base"), 

            F.col("b.phy_rate_score_2_4g").alias("phy_rate_score_2_4g_base"),
            F.col("b.phy_rate_score_5g").alias("phy_rate_score_5g_base"),
            F.col("b.phy_rate_score_6g").alias("phy_rate_score_6g_base"),

            F.col("v.snr_range").alias("snr_range_variation"),
            F.col("v.son_count").alias("son_count_variation"),
            F.col("v.phy_rate_range_2_4g").alias("phy_rate_range_2_4g_variation"),
            F.col("v.phy_rate_range_5g").alias("phy_rate_range_5g_variation"),
            F.col("base_score").alias("base_score"),
            F.col("v.variation_score").alias("variation_score"),
            F.when(score_val >= F.lit(4), F.lit("Excellent"))
            .when(score_val == F.lit(3), F.lit("Good"))
            .when(score_val == F.lit(2), F.lit("Fair"))
            .otherwise(F.lit("Poor"))
            .alias("station_score"),
            F.when(score_val >= F.lit(4), F.lit(4))
            .when(score_val == F.lit(3), F.lit(3))
            .when(score_val == F.lit(2), F.lit(2))
            .otherwise(F.lit(1))
            .alias("station_score_num"),
        )
        '''
        output_df = output_df.filter(
            ~(
                F.col("p90_phy_rate_2_4g_base").isNull()
                & F.col("p90_phy_rate_5g_base").isNull()
                & F.col("p90_phy_rate_6g_base").isNull()
                & F.col("p90_rssi_2_4g_base").isNull()
                & F.col("p90_rssi_5g_base").isNull()
                & F.col("p90_rssi_6g_base").isNull()
            )
        )
        '''

        any_rssi_sentinel = (
            (F.coalesce(F.col("p90_rssi_2_4g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p90_rssi_5g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p90_rssi_6g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p50_rssi_2_4g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p50_rssi_5g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p50_rssi_6g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p95_rssi_2_4g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p95_rssi_5g_base"), F.lit(0)) == -100)
            | (F.coalesce(F.col("p95_rssi_6g_base"), F.lit(0)) == -100)
        )

        # 2. Update PHY sentinel with the correct "_base" suffix
        any_phy_sentinel = (
            (F.coalesce(F.col("p90_phy_rate_2_4g_base"), F.lit(-1)) == 0)
            | (F.coalesce(F.col("p90_phy_rate_5g_base"), F.lit(-1)) == 0)
            | (F.coalesce(F.col("p90_phy_rate_6g_base"), F.lit(-1)) == 0)
        )
        output_df = output_df.filter(~(any_rssi_sentinel & any_phy_sentinel))

        output_df.write.mode("overwrite").parquet(f"{self.output_path}/date={self.date_str}/hour={self.hour_str}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName('station_score_hourly')\
                        .config("spark.ui.port","24049")\
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