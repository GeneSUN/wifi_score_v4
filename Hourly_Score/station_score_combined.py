"""
station_score_combined.py

Takes raw per-record station data (the schema shown below) as input,
derives all intermediate columns, and outputs the hourly WiFi score.

Input schema required:
  sn, rowkey, station_mac, station_name, mode, band,
  rssi, snr, phy_rate, tx_phy_rate, model, hour, date

Usage (diagnostic / interactive):
  from station_score_combined import StationScorePipeline
  result = StationScorePipeline(spark).run(df)
  result.show()
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F


class StationScorePipeline:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    # ------------------------------------------------------------------
    # Step 1: derive p90_rssi + mobility_status from raw records
    # ------------------------------------------------------------------
    def _enrich(self, df: DataFrame) -> DataFrame:
        # p90_rssi per station_mac (replicates StationConnection logic)
        w_mac = Window.partitionBy("station_mac").rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing
        )
        df = (
            df
            .withColumn("p90_rssi", F.percentile_approx("rssi", 0.9).over(w_mac).cast("int"))
            .withColumn("p50_rssi", F.percentile_approx("rssi", 0.5).over(w_mac).cast("int"))
        )

        # mobility_status: p90-p50 diff <= 5 → Stationary (stationarity algo)
        df = df.withColumn(
            "mobility_status",
            F.when((F.col("p90_rssi") - F.col("p50_rssi")) <= 5, "Stationary")
             .otherwise("Non-stationary")
        )

        # son: no steering data in raw input → default 0
        if "son" not in df.columns:
            df = df.withColumn("son", F.lit(0))

        return df

    # ------------------------------------------------------------------
    # Step 2: filter + score
    # ------------------------------------------------------------------
    def _score(self, df: DataFrame) -> DataFrame:
        # Remove invalid records
        df = (
            df
            .filter(~(
                (F.col("phy_rate").cast("double") == 0.0)
                & (F.col("rssi").cast("double") == 0.0)
                & (F.col("p90_rssi").cast("double") == -100.0)
            ))
            .filter(F.col("p90_rssi").isNotNull() | F.col("phy_rate").isNotNull() | F.col("snr").isNotNull())
            .filter(~F.col("band").isin("ETH", "Ether", "ethernet") & F.col("band").isNotNull())
        )

        # Require >= 9 records per (sn, station_mac, date, hour)
        w = Window.partitionBy("sn", "station_mac", "date", "hour")
        df_filt = (
            df
            .withColumn("record_count", F.count(F.lit(1)).over(w))
            .filter(F.col("record_count") >= 9)
        )

        # Pivot: RSSI + PHY rate percentiles per band
        pivoted = (
            df_filt.groupBy("sn", "station_mac", "date", "hour")
            .agg(
                F.max("model").alias("model"),
                F.max("mobility_status").alias("mobility_status"),
                F.max("station_name").alias("station_name"),
                F.max("mode").alias("mode"),
                # RSSI percentiles
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"),   F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"),   F.col("p90_rssi").cast("double")), 0.90).alias("p90_rssi_6g"),
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"),   F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"),   F.col("p90_rssi").cast("double")), 0.50).alias("p50_rssi_6g"),
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"),   F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"),   F.col("p90_rssi").cast("double")), 0.95).alias("p95_rssi_6g"),
                # PHY rate percentiles
                F.percentile_approx(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_2_4g"),
                F.percentile_approx(F.when(F.col("band").like("5G%"),   F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_5g"),
                F.percentile_approx(F.when(F.col("band").like("6G%"),   F.col("phy_rate").cast("double")), 0.9).alias("p90_phy_rate_6g"),
            )
        )

        # Score helpers
        def rssi_score(col_name, good, fair, poor):
            c = F.col(col_name)
            return (
                F.when(c >= good, 4)
                .when(c.between(fair, good - 1), 3)
                .when(c.between(poor, fair - 1), 2)
                .when(c < poor, 1)
            )

        def phy_score(col_name, high, mid, low):
            c = F.col(col_name)
            m = F.col("mode")
            legacy_cap = (
                ((m == 0)        & (c <= 2))
                | (m.isin(1, 2)  & (c <= 11))
                | ((m == 3)      & (c <= 24))
                | (m.between(4, 7)  & (c <= 12))
                | (m.between(8, 29) & (c <= 6))
            )
            return (
                F.when(legacy_cap, 4)
                .when(c > high, 4)
                .when(c.between(mid, high), 3)
                .when(c.between(low, mid - 1), 2)
                .when(c < low, 1)
            )

        base_scores = (
            pivoted
            .withColumn("rssi_score_2_4g",    rssi_score("p90_rssi_2_4g", -65, -75, -85))
            .withColumn("rssi_score_5g",       rssi_score("p90_rssi_5g",   -68, -78, -88))
            .withColumn("rssi_score_6g",       rssi_score("p90_rssi_6g",   -62, -72, -82))
            .withColumn("phy_rate_score_2_4g", phy_score("p90_phy_rate_2_4g", 144, 72,  12))
            .withColumn("phy_rate_score_5g",   phy_score("p90_phy_rate_5g",   600, 300, 86))
            .withColumn("phy_rate_score_6g",   phy_score("p90_phy_rate_6g",  1200, 600, 286))
        )

        BIG = 10**9

        def least_non_null(*cols):
            return F.least(*[F.coalesce(F.col(c), F.lit(BIG)) for c in cols])

        def null_if_all_null(result_col, *source_cols):
            all_null = F.col(source_cols[0]).isNull()
            for c in source_cols[1:]:
                all_null = all_null & F.col(c).isNull()
            return F.when(all_null, F.lit(None)).otherwise(F.col(result_col))

        combined = (
            base_scores
            .withColumn("final_rssi_score",    least_non_null("rssi_score_2_4g",     "rssi_score_5g",     "rssi_score_6g"))
            .withColumn("final_phy_rate_score", least_non_null("phy_rate_score_2_4g", "phy_rate_score_5g", "phy_rate_score_6g"))
            .withColumn("final_rssi_score",     null_if_all_null("final_rssi_score",     "rssi_score_2_4g",     "rssi_score_5g",     "rssi_score_6g"))
            .withColumn("final_phy_rate_score",  null_if_all_null("final_phy_rate_score", "phy_rate_score_2_4g", "phy_rate_score_5g", "phy_rate_score_6g"))
            .filter(F.col("final_rssi_score").isNotNull() | F.col("final_phy_rate_score").isNotNull())
        )

        # Variation score
        variation_cal = (
            df_filt.groupBy("sn", "station_mac", "date", "hour")
            .agg(
                (F.max(F.col("snr").cast("double")) - F.min(F.col("snr").cast("double"))).alias("snr_range"),
                F.sum(F.when(F.col("son") >= 1, 1).otherwise(0)).alias("son_count"),
                (F.max(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")))
                 - F.min(F.when(F.col("band").like("2.4G%"), F.col("phy_rate").cast("double")))).alias("phy_rate_range_2_4g"),
                (F.max(F.when(F.col("band").like("5G%"),   F.col("phy_rate").cast("double")))
                 - F.min(F.when(F.col("band").like("5G%"),   F.col("phy_rate").cast("double")))).alias("phy_rate_range_5g"),
            )
            .withColumn(
                "variation_score",
                F.when(
                    (F.col("snr_range") > 13)
                    | (F.col("son_count") > 6)
                    | (F.col("phy_rate_range_2_4g") > 30)
                    | (F.col("phy_rate_range_5g") > 300),
                    1,
                ).otherwise(0)
            )
        )

        joined = (
            combined.alias("b")
            .join(variation_cal.alias("v"), on=["sn", "station_mac", "date", "hour"], how="inner")
            .withColumn("base_score", F.least(F.col("b.final_rssi_score"), F.col("b.final_phy_rate_score")))
        )

        score_val = F.col("base_score") - F.col("v.variation_score")

        return joined.select(
            F.col("b.sn"),
            F.col("b.station_mac"),
            F.col("b.date"),
            F.col("b.hour"),
            F.col("b.mobility_status"),
            F.col("b.mode"),
            # RSSI
            F.col("b.p90_rssi_2_4g"),
            F.col("b.p90_rssi_5g"),
            F.col("b.p90_rssi_6g"),
            # PHY rate
            F.col("b.p90_phy_rate_2_4g"),
            F.col("b.p90_phy_rate_5g"),
            F.col("b.p90_phy_rate_6g"),
            # Scores per band
            F.col("b.rssi_score_2_4g"),
            F.col("b.rssi_score_5g"),
            F.col("b.rssi_score_6g"),
            F.col("b.phy_rate_score_2_4g"),
            F.col("b.phy_rate_score_5g"),
            F.col("b.phy_rate_score_6g"),
            # Variation
            F.col("v.snr_range"),
            F.col("v.phy_rate_range_2_4g"),
            F.col("v.phy_rate_range_5g"),
            F.col("v.variation_score"),
            # Final
            F.col("base_score"),
            F.when(score_val >= 4, "Excellent").when(score_val == 3, "Good").when(score_val == 2, "Fair").otherwise("Poor").alias("station_score"),
            F.when(score_val >= 4, 4).when(score_val == 3, 3).when(score_val == 2, 2).otherwise(1).alias("station_score_num"),
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------
    def run(self, df: DataFrame) -> DataFrame:
        df = self._enrich(df)
        return self._score(df)
