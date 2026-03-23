import sys
import traceback
import argparse
from datetime import datetime, timedelta, timezone
from functools import reduce

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import FloatType, StringType, StructType
from pyspark.sql.utils import AnalysisException

sys.path.append('/usr/apps/vmas/scripts/ZS')
from MailSender import MailSender

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------
HDFS_PD = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
HDFS_PA = "hdfs://njbbepapa1.nss.vzwnet.com:9000"


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def path_exists(path: str) -> bool:
    """Return True if an HDFS path exists, False otherwise."""
    try:
        spark.read.text(path).limit(1).count()
        return True
    except AnalysisException:
        return False


def flatten_df_v2(nested_df: DataFrame) -> DataFrame:
    """Recursively flatten struct and array columns into top-level columns."""
    flat_cols   = [c for c, t in nested_df.dtypes if not t.startswith("struct") and not t.startswith("array")]
    nested_cols = [c for c, t in nested_df.dtypes if t.startswith("struct")]
    array_cols  = [c for c, t in nested_df.dtypes if t.startswith("array")]

    if not nested_cols and not array_cols:
        return nested_df

    if nested_cols:
        expanded = [
            F.col(f"{nc}.{c}").alias(f"{nc}_b_{c}")
            for nc in nested_cols
            for c in nested_df.select(f"{nc}.*").columns
        ]
        return flatten_df_v2(nested_df.select(flat_cols + expanded))

    # array columns
    for array_col in array_cols:
        nested_df = nested_df.withColumn(array_col, F.explode(F.col(array_col)))
    return flatten_df_v2(nested_df)


def read_parquet_for_date_range(date_range: list, path_template: str) -> DataFrame:
    """
    Read parquet files for a list of date strings.
    Uses a single multi-path read instead of reduce(union) to avoid a deeply
    nested query plan.
    """
    paths = [path_template.format(d) for d in date_range]
    existing = [p for p in paths if path_exists(p)]
    if not existing:
        raise FileNotFoundError(f"No parquet files found for template: {path_template}")
    return spark.read.parquet(*existing)


def replace_rows(base_df: DataFrame, update_df: DataFrame, key_col: str = "sn") -> DataFrame:
    """
    Replace rows in base_df with rows from update_df when key_col matches.
    Rows in update_df take precedence over rows in base_df.
    """
    update_keys   = update_df.select(key_col).distinct()
    base_remaining = base_df.join(update_keys, on=key_col, how="left_anti")
    return base_remaining.unionByName(update_df)


# ---------------------------------------------------------------------------
# Category helper
# ---------------------------------------------------------------------------

def numeric_to_category(col_name: str) -> F.Column:
    """Map a 1-4 numeric score column to Poor / Fair / Good / Excellent."""
    c = F.col(col_name)
    return (
        F.when(c <  1.5, "Poor")
         .when(c <  2.5, "Fair")
         .when(c <  3.5, "Good")
         .when(c >= 3.5, "Excellent")
         .otherwise(None)
    )


def category_to_numeric(col_name: str) -> F.Column:
    """Map a Poor/Fair/Good/Excellent category column to 1/2/3/4."""
    c = F.col(col_name)
    return (
        F.when(c == "Poor",      1)
         .when(c == "Fair",      2)
         .when(c == "Good",      3)
         .when(c == "Excellent", 4)
         .otherwise(None)
    )


def normalize_connect_type(col_name: str) -> F.Column:
    """Normalize raw connect_type strings to '2_4G', '5G', or '6G'."""
    c = F.col(col_name)
    return (
        F.when(c.like("2.4G%"), "2_4G")
         .when(c.like("5G%"),   "5G")
         .when(c.like("6G%"),   "6G")
         .otherwise(c)
    )


def categorize_reboots(col_name: str) -> F.Column:
    """Classify reboot / modem-reset count into Poor/Fair/Good/Excellent."""
    c = F.col(col_name)
    return (
        F.when(c >= 5,              "Poor")
         .when(c == 4,              "Fair")
         .when(c.between(2, 3),     "Good")
         .when(c >= 0,              "Excellent")
         .when(c.isNull(),          "Excellent")
         .otherwise(None)
    )


# ---------------------------------------------------------------------------
# Main analysis class
# ---------------------------------------------------------------------------

class WifiKPIAnalysis:

    # HDFS path templates for daily intermediates
    IP_CHANGE_TEMPLATE  = HDFS_PD + "/user/ZheS/wifi_score_v4/time_window/{}/ip_change_daily_df/"
    AIRTIME_TEMPLATE    = HDFS_PD + "/user/ZheS/wifi_score_v4/time_window/{}/airtime_daily_df/"
    RESTART_TEMPLATE    = HDFS_PD + "/user/ZheS/wifi_score_v4/time_window/{}/restart_daily_df/"

    def __init__(self,
                 date_val,
                 owl_path,
                 station_history_path,
                 deviceGroup_path,
                 df_rssi_path,
                 df_phyrate_path,
                 wifiscore_path):
        self.date_val             = date_val
        self.owl_path             = owl_path
        self.station_history_path = station_history_path
        self.deviceGroup_path     = deviceGroup_path
        self.df_rssi_path         = df_rssi_path
        self.df_phyrate_path      = df_phyrate_path
        self.wifiscore_path       = wifiscore_path

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def load_data(self):
        self.model_sn_df = (
            spark.read.parquet(self.deviceGroup_path)
                 .withColumn("features", F.explode("Group_Data_sys_info"))
                 .select(
                     "features.sn",
                     F.col("features.model").alias("model_name"),
                     F.col("Tplg_Data_fw_ver").alias("firmware"),
                 )
                 .distinct()
                 .groupBy("sn", "model_name")
                 .agg(F.max("firmware").alias("firmware"))
        )

        self.df_dg = (
            spark.read.parquet(self.deviceGroup_path)
                 .withColumn("sn", F.regexp_extract(F.col("rowkey"), r"-(\w+)", 1))
        )

        self.df_owl = (
            spark.read.parquet(self.owl_path)
                 .withColumn("sn",       F.regexp_extract(F.col("rowkey"), r"-(\w+)_", 1))
                 .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
                 .cache()   # reused across multiple methods
        )

        self.df_sh = (
            spark.read.parquet(self.station_history_path)
                 .withColumn("sn",       F.regexp_extract(F.col("rowkey"), r"-(\w+)_", 1))
                 .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
                 .cache()   # reused across multiple methods
        )

    # ------------------------------------------------------------------
    # KPI: IP changes
    # ------------------------------------------------------------------

    def calculate_ip_changes(self) -> DataFrame:
        previous_day = self.date_val - timedelta(1)
        date_range   = [(previous_day - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]

        ip_change_df = (
            read_parquet_for_date_range(date_range, self.IP_CHANGE_TEMPLATE)
                .groupBy("sn")
                .agg(F.sum("no_ip_changes").alias("no_ip_changes"))
                .withColumn(
                    "ip_change_category",
                    F.when(F.col("no_ip_changes") > 6,             "Poor")
                     .when(F.col("no_ip_changes").between(4, 6),   "Fair")
                     .when(F.col("no_ip_changes").between(1, 3),   "Good")
                     .when(F.col("no_ip_changes") == 0,            "Excellent")
                     .otherwise("Unknown"),
                )
        )
        return ip_change_df

    # ------------------------------------------------------------------
    # KPI: Airtime utilization
    # ------------------------------------------------------------------

    def calculate_airtime(self) -> DataFrame:
        date_range = [(self.date_val - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]

        airtime_df = (
            read_parquet_for_date_range(date_range, self.AIRTIME_TEMPLATE)
                .groupBy("sn")
                .agg(F.sum("no_poor_airtime").alias("no_poor_airtime"))
                .withColumn(
                    "Airtime_Utilization_Category",
                    F.when(F.col("no_poor_airtime") > 200,                                  "Poor")
                     .when(F.col("no_poor_airtime").between(75, 200),                       "Fair")
                     .when(F.col("no_poor_airtime").between(26, 74),                        "Good")
                     .when(F.col("no_poor_airtime") >= 0,                                   "Excellent")
                     .otherwise("Unknown"),
                )
        )
        return airtime_df

    # ------------------------------------------------------------------
    # KPI: Band / AP steering
    # ------------------------------------------------------------------

    def calculate_steer(self) -> DataFrame:
        df_sh = self.df_sh

        # --- band steering counts ---
        df_bandsteer = (
            df_sh.select("sn",
                         F.col("Diag_Result_band_steer.sta_type").alias("sta_type"),
                         F.col("Diag_Result_band_steer.action").alias("action"))
                 .filter(F.col("sta_type") == "2")
                 .groupBy("sn")
                 .agg(F.count(F.when(F.col("action") == "2", True)).alias("band_success_count"))
        )

        # --- AP steering counts ---
        df_apsteer = (
            df_sh.select("sn",
                         F.col("Diag_Result_ap_steer.sta_type").alias("sta_type"),
                         F.col("Diag_Result_ap_steer.action").alias("action"))
                 .filter(F.col("sta_type") == "2")
                 .groupBy("sn")
                 .agg(F.count(F.when(F.col("action") == "2", True)).alias("ap_success_count"))
        )

        # --- distinct rowkey count per SN ---
        df_rowkey_count = (
            df_sh.groupBy("sn")
                 .agg(F.countDistinct("rowkey").alias("distinct_rowkey_count"))
        )

        steer_df = (
            df_bandsteer
                .join(df_apsteer, on="sn", how="full_outer")
                .withColumn(
                    "steer_start_count",
                    F.coalesce(F.col("band_success_count"), F.lit(0))
                    + F.coalesce(F.col("ap_success_count"),   F.lit(0)),
                )
                .withColumn(
                    "steer_start_category",
                    F.when(F.col("steer_start_count") > 60,               "Poor")
                     .when(F.col("steer_start_count").between(31, 60),    "Fair")
                     .when(F.col("steer_start_count").between(11, 30),    "Good")
                     .when(F.col("steer_start_count").isNull()
                           | (F.col("steer_start_count") <= 10),          "Excellent")
                     .otherwise("Unknown"),
                )
                .join(df_rowkey_count, on="sn")
                .withColumn(
                    "steer_start_perHome_count",
                    F.col("steer_start_count") / F.col("distinct_rowkey_count"),
                )
                .withColumn(
                    "steer_start_perHome_category",
                    F.when(F.col("steer_start_perHome_count") > 3,            "Poor")
                     .when(F.col("steer_start_perHome_count") > 2,            "Fair")
                     .when(F.col("steer_start_perHome_count") > 1,            "Good")
                     .when(F.col("steer_start_perHome_count").isNull()
                           | (F.col("steer_start_perHome_count") <= 1),       "Excellent")
                     .otherwise("Unknown"),
                )
        )
        return steer_df

    # ------------------------------------------------------------------
    # KPI: Restarts / reboots
    # ------------------------------------------------------------------

    def calculate_restart(self) -> DataFrame:
        date_range = [(self.date_val - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]

        restart_df = (
            read_parquet_for_date_range(date_range, self.RESTART_TEMPLATE)
                .na.fill(0, subset=["num_mdm_crashes", "num_user_reboots", "num_total_reboots"])
                .groupBy("sn")
                .agg(
                    F.sum("num_mdm_crashes").alias("mdm_resets_count"),
                    F.sum("num_user_reboots").alias("user_reboot_count"),
                    F.sum("num_total_reboots").alias("total_reboots_count"),
                )
                .withColumn("reboot_category",          categorize_reboots("total_reboots_count"))
                .withColumn("modem_reset_category",     categorize_reboots("mdm_resets_count"))
                .withColumn(
                    "customer_reboot_category",
                    F.when(F.col("user_reboot_count") >= 3,  "Poor")
                     .when(F.col("user_reboot_count") == 2,  "Fair")
                     .when(F.col("user_reboot_count") == 1,  "Good")
                     .when(F.col("user_reboot_count") == 0,  "Excellent")
                     .when(F.col("user_reboot_count").isNull(), "Excellent")
                     .otherwise(None),
                )
        )
        return restart_df

    # ------------------------------------------------------------------
    # KPI: RSSI (signal strength)
    # ------------------------------------------------------------------

    def calculate_rssi(self) -> DataFrame:
        df_sh = self.df_sh

        RSSI_THRESHOLDS = {
            "2_4GHz": {"Poor": -78, "Fair": -71, "Good": -56},
            "5GHz":   {"Poor": -75, "Fair": -71, "Good": -56},
            "6GHz":   {"Poor": -70, "Fair": -65, "Good": -56},
        }

        def add_rssi_category(df: DataFrame, signal_col: str, band: str) -> DataFrame:
            t = RSSI_THRESHOLDS[band]
            return df.withColumn(
                f"category_{band}",
                F.when(F.col(signal_col) < t["Poor"],                                "Poor")
                 .when(F.col(signal_col).between(t["Poor"],  t["Fair"]),             "Fair")
                 .when(F.col(signal_col).between(t["Fair"] - 1, t["Good"]),          "Good")
                 .when(F.col(signal_col) > t["Good"],                                "Excellent")
                 .otherwise("No Data"),
            )

        df_flat = (
            df_sh.withColumn("connect_type",    F.col("Station_Data_connect_data.connect_type"))
                 .withColumn("connect_type",    normalize_connect_type("connect_type"))
                 .withColumn("signal_strength", F.col("Station_Data_connect_data.signal_strength"))
                 .withColumn("byte_send",       F.col("Station_Data_connect_data.diff_bs"))
                 .withColumn("byte_received",   F.col("Station_Data_connect_data.diff_br"))
                 .filter(F.col("signal_strength").isNotNull())
                 .withColumn("signal_strength_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("signal_strength")))
                 .withColumn("signal_strength_5GHz",   F.when(F.col("connect_type") == "5G",   F.col("signal_strength")))
                 .withColumn("signal_strength_6GHz",   F.when(F.col("connect_type") == "6G",   F.col("signal_strength")))
                 .select("sn", "rowkey", "ts", "connect_type", "signal_strength",
                         "signal_strength_2_4GHz", "signal_strength_5GHz", "signal_strength_6GHz",
                         "byte_send", "byte_received")
        )

        df_cat = add_rssi_category(df_flat, "signal_strength_2_4GHz", "2_4GHz")
        df_cat = add_rssi_category(df_cat,  "signal_strength_5GHz",   "5GHz")
        df_cat = add_rssi_category(df_cat,  "signal_strength_6GHz",   "6GHz")

        df_grouped = (
            df_cat.groupBy("sn", "rowkey")
                  .agg(
                      F.count("*").alias("total_count_rssi"),
                      *[
                          F.sum(F.when(F.col(f"category_{band}") == lvl, 1).otherwise(0))
                           .alias(f"{lvl.lower()}_count_{band}")
                          for band in ["2_4GHz", "5GHz", "6GHz"]
                          for lvl in ["Poor", "Fair", "Good", "Excellent"]
                      ],
                      F.sum("byte_send").alias("byte_send"),
                      F.sum("byte_received").alias("byte_received"),
                  )
        )

        volume_window = Window.partitionBy("sn")
        df_rowkey = (
            df_grouped
                .withColumn(
                    "rssi_category_rowkey",
                    F.when((F.col("poor_count_2_4GHz")      >= 12) | (F.col("poor_count_5GHz")      >= 12) | (F.col("poor_count_6GHz")      >= 12), "Poor")
                     .when((F.col("fair_count_2_4GHz")      >= 12) | (F.col("fair_count_5GHz")      >= 12) | (F.col("fair_count_6GHz")      >= 12), "Fair")
                     .when((F.col("good_count_2_4GHz")      >= 12) | (F.col("good_count_5GHz")      >= 12) | (F.col("good_count_6GHz")      >= 12), "Good")
                     .when((F.col("excellent_count_2_4GHz") >= 12) | (F.col("excellent_count_5GHz") >= 12) | (F.col("excellent_count_6GHz") >= 12), "Excellent")
                     .otherwise("No Data"),
                )
                .withColumn("volume",       F.log(F.col("byte_send") + F.col("byte_received")))
                .withColumn("total_volume", F.sum("volume").over(volume_window))
                .withColumn("weights",      F.col("volume") / F.col("total_volume"))
        )

        # Save per-rowkey detail (best-effort)
        try:
            self._build_connect_type_df()
            (
                df_rowkey.join(self.df_connectType, ["sn", "rowkey"], "left")
                         .withColumn("date", F.lit(self.date_val.strftime("%Y-%m-%d")))
                         .write.mode("overwrite").parquet(self.df_rssi_path)
            )
        except Exception as e:
            print(f"[WARN] Failed to save df_rowkey_rssi_category: {e}")

        # Weighted average numeric score → final category
        df_final = (
            df_rowkey
                .withColumn("rssi_numeric_rowkey", category_to_numeric("rssi_category_rowkey"))
                .groupBy("sn")
                .agg(F.round(F.sum(F.col("rssi_numeric_rowkey") * F.col("weights")), 4).alias("rssi_numeric"))
                .withColumn("RSSI_category", numeric_to_category("rssi_numeric"))
        )
        return df_final

    # ------------------------------------------------------------------
    # KPI: PHY rate (link rate)
    # ------------------------------------------------------------------

    def calculate_phyrate(self) -> DataFrame:
        df_sh = self.df_sh

        PHYRATE_THRESHOLDS = {
            "2_4GHz": {"Poor": 80,  "Fair": 100, "Good": 120},
            "5GHz":   {"Poor": 200, "Fair": 300, "Good": 500},
            "6GHz":   {"Poor": 200, "Fair": 300, "Good": 500},
        }

        def add_phyrate_category(df: DataFrame, rate_col: str, band: str) -> DataFrame:
            t = PHYRATE_THRESHOLDS[band]
            return df.withColumn(
                f"category_{band}",
                F.when(F.col(rate_col) < t["Poor"],                         "Poor")
                 .when(F.col(rate_col) < t["Fair"],                         "Fair")
                 .when(F.col(rate_col) < t["Good"],                         "Good")
                 .when(F.col(rate_col) >= t["Good"],                        "Excellent")
                 .otherwise("No Data"),
            )

        df_flat = (
            df_sh.withColumn("connect_type",  F.col("Station_Data_connect_data.connect_type"))
                 .withColumn("connect_type",  normalize_connect_type("connect_type"))
                 .filter(F.col("connect_type").isin("2_4G", "5G", "6G"))
                 .withColumn("byte_send",     F.col("Station_Data_connect_data.diff_bs"))
                 .withColumn("byte_received", F.col("Station_Data_connect_data.diff_br"))
                 .withColumn("tx_link_rate",  F.regexp_replace(F.col("Station_Data_connect_data.tx_link_rate"), "Mbps", ""))
                 .withColumn("tx_link_rate_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("tx_link_rate")))
                 .withColumn("tx_link_rate_5GHz",   F.when(F.col("connect_type") == "5G",   F.col("tx_link_rate")))
                 .withColumn("tx_link_rate_6GHz",   F.when(F.col("connect_type") == "6G",   F.col("tx_link_rate")))
                 .select("sn", "rowkey", "ts", "connect_type", "tx_link_rate",
                         "tx_link_rate_2_4GHz", "tx_link_rate_5GHz", "tx_link_rate_6GHz",
                         "byte_send", "byte_received")
        )

        df_cat = add_phyrate_category(df_flat, "tx_link_rate_2_4GHz", "2_4GHz")
        df_cat = add_phyrate_category(df_cat,  "tx_link_rate_5GHz",   "5GHz")
        df_cat = add_phyrate_category(df_cat,  "tx_link_rate_6GHz",   "6GHz")

        df_grouped = (
            df_cat.groupBy("sn", "rowkey")
                  .agg(
                      F.sum("byte_send").alias("byte_send"),
                      F.sum("byte_received").alias("byte_received"),
                      F.count("*").alias("total_count_phyrate"),
                      *[
                          F.sum(F.when(F.col(f"category_{band}") == lvl, 1).otherwise(0))
                           .alias(f"{lvl.lower()}_count_{band}")
                          for band in ["2_4GHz", "5GHz", "6GHz"]
                          for lvl in ["Poor", "Fair", "Good", "Excellent"]
                      ],
                  )
        )

        # Label each rowkey by the dominant quality tier (needs >= 12 samples in a tier)
        volume_window = Window.partitionBy("sn")
        df_rowkey = (
            df_grouped
                .withColumn(
                    "rowkey_phyrate_category",
                    F.when((F.col("poor_count_2_4GHz")      >= 12) | (F.col("poor_count_5GHz")      >= 12) | (F.col("poor_count_6GHz")      >= 12), "Poor")
                     .when((F.col("fair_count_2_4GHz")      >= 12) | (F.col("fair_count_5GHz")      >= 12) | (F.col("fair_count_6GHz")      >= 12), "Fair")
                     .when((F.col("good_count_2_4GHz")      >= 12) | (F.col("good_count_5GHz")      >= 12) | (F.col("good_count_6GHz")      >= 12), "Good")
                     .when((F.col("excellent_count_2_4GHz") >= 12) | (F.col("excellent_count_5GHz") >= 12) | (F.col("excellent_count_6GHz") >= 12), "Excellent")
                     .otherwise("No Data"),
                )
                .withColumn("volume",       F.log(F.col("byte_send") + F.col("byte_received")))
                .withColumn("total_volume", F.sum("volume").over(volume_window))
                .withColumn("weights",      F.col("volume") / F.col("total_volume"))
        )

        # Save per-rowkey detail (best-effort)
        try:
            (
                df_rowkey.join(self.df_connectType, ["sn", "rowkey"], "left")
                         .withColumn("date", F.lit(self.date_val.strftime("%Y-%m-%d")))
                         .write.mode("overwrite").parquet(self.df_phyrate_path)
            )
        except Exception as e:
            print(f"[WARN] Failed to save df_rowkey_phyrate_category: {e}")

        df_final = (
            df_rowkey
                .withColumn("phyrate_numeric_rowkey", category_to_numeric("rowkey_phyrate_category"))
                .groupBy("sn")
                .agg(F.round(F.sum(F.col("phyrate_numeric_rowkey") * F.col("weights")), 4).alias("phyrate_numeric"))
                .withColumn("phyrate_category", numeric_to_category("phyrate_numeric"))
        )
        return df_final

    # ------------------------------------------------------------------
    # KPI: Sudden signal drop
    # ------------------------------------------------------------------

    def calculate_sudden_drop(self) -> DataFrame:
        df_sh = self.df_sh

        df = df_sh.withColumn("signal_strength", F.col("Station_Data_connect_data.signal_strength"))\
                  .filter(F.col("signal_strength").isNotNull())

        partition_cols = ["sn", "rowkey"]
        window_spec    = Window.partitionBy(partition_cols)

        # Compute percentiles once per (sn, rowkey) partition
        df_pct = (
            df.withColumn("p03", F.expr("percentile_approx(signal_strength, 0.03)").over(window_spec))
              .withColumn("p10", F.expr("percentile_approx(signal_strength, 0.1)").over(window_spec))
              .withColumn("p50", F.expr("percentile_approx(signal_strength, 0.5)").over(window_spec))
              .withColumn("p90", F.expr("percentile_approx(signal_strength, 0.9)").over(window_spec))
              .withColumn("lower_bound", F.col("p10") - 2 * (F.col("p90") - F.col("p10")))
              .withColumn("outlier",     F.when(F.col("lower_bound") < F.col("p03"), F.col("lower_bound")).otherwise(F.col("p03")))
              .filter(F.col("signal_strength") > F.col("outlier"))
        )

        # Identify stationary devices (90th and 50th pct are within 5 dBm)
        stationary_df = (
            df_pct.withColumn("stationarity", F.when((F.col("p90") - F.col("p50")) <= 5, "1").otherwise("0"))
                  .groupBy(partition_cols)
                  .agg(F.max("stationarity").alias("stationarity"))
                  .filter(F.col("stationarity") > 0)
        )

        # Count connected stations over time for stationary devices
        df_ts = (
            df_sh.join(stationary_df, ["sn", "rowkey"])
                 .select("sn", "rowkey", "datetime",
                         F.col("Station_Data_connect_data.station_mac").alias("station_mac"))
                 .filter(F.col("station_mac").isNotNull())
                 .groupBy("sn", "datetime")
                 .agg(F.count("station_mac").alias("station_cnt"))
        )

        time_window = Window.partitionBy("sn").orderBy("datetime")
        df_result = (
            df_ts.withColumn("prev_station_cnt", F.lag("station_cnt").over(time_window))
                 .withColumn("drop_diff", F.col("prev_station_cnt") - F.col("station_cnt"))
                 .filter(F.col("drop_diff") > 3)
                 .groupBy("sn")
                 .agg(F.count("drop_diff").alias("no_sudden_drop"))
                 .withColumn(
                     "sudden_drop_category",
                     F.when(F.col("no_sudden_drop") >= 2,      "Poor")
                      .when(F.col("no_sudden_drop") == 1,      "Fair")
                      .when(F.col("no_sudden_drop").isNull(),  "Excellent")
                      .otherwise("Good"),
                 )
        )
        return df_result

    # ------------------------------------------------------------------
    # Helper: connect-type lookup table (built once, reused by rssi & phyrate)
    # ------------------------------------------------------------------

    def _build_connect_type_df(self):
        if hasattr(self, "df_connectType"):
            return  # already built

        window_spec = Window.partitionBy("sn", "rowkey").orderBy(F.desc("count"))
        self.df_connectType = (
            self.df_sh.select(
                    "rowkey",
                    F.col("Station_Data_connect_data.station_name").alias("station_name"),
                    F.col("Station_Data_connect_data.connect_type").alias("connect_type"),
                )
                .withColumn("sn",           F.regexp_extract("rowkey", r"([A-Z]+\d+)", 1))
                .withColumn("connect_type", normalize_connect_type("connect_type"))
                .filter(F.col("station_name").isNotNull() & F.col("connect_type").isNotNull())
                .groupBy("sn", "rowkey", "connect_type", "station_name").count()
                .withColumn("rank", F.rank().over(window_spec))
                .filter(F.col("rank") == 1)
                .drop("rank", "count")
        )

    # ------------------------------------------------------------------
    # Join all KPIs
    # ------------------------------------------------------------------

    def _join_all(self):
        ip_change_df  = self.calculate_ip_changes()
        steer_df      = self.calculate_steer()
        restart_df    = self.calculate_restart()
        rssi_df       = self.calculate_rssi()
        phyrate_df    = self.calculate_phyrate()
        airtime_df    = self.calculate_airtime()
        sudden_drop_df = self.calculate_sudden_drop()

        category_cols = [
            "ip_change_category", "steer_start_category", "steer_start_perHome_category",
            "customer_reboot_category", "reboot_category", "modem_reset_category",
            "sudden_drop_category", "Airtime_Utilization_Category",
        ]
        numeric_cols = [
            "no_ip_changes", "steer_start_count", "steer_start_perHome_count",
            "user_reboot_count", "total_reboots_count", "mdm_resets_count",
            "no_sudden_drop", "no_poor_airtime",
        ]

        self.df_full_joined = (
            ip_change_df
                .join(steer_df,       on="sn", how="full_outer")
                .join(restart_df,     on="sn", how="full_outer")
                .join(rssi_df,        on="sn", how="full_outer")
                .join(phyrate_df,     on="sn", how="full_outer")
                .join(airtime_df,     on="sn", how="full_outer")
                .join(sudden_drop_df, on="sn", how="full_outer")
                .withColumn("day", F.lit(F.date_format(F.lit(self.date_val), "MM-dd-yyyy")))
                .na.fill("Excellent", subset=category_cols)
                .na.fill(0,           subset=numeric_cols)
                .join(self.model_sn_df, on="sn")
        )

    # ------------------------------------------------------------------
    # Score calculation
    # ------------------------------------------------------------------

    def get_score(self):
        self.load_data()
        self._join_all()

        categorical_columns = [
            "ip_change_category", "steer_start_category", "steer_start_perHome_category",
            "customer_reboot_category", "reboot_category", "modem_reset_category",
            "sudden_drop_category", "RSSI_category", "phyrate_category",
            "Airtime_Utilization_Category",
        ]

        # Convert all category columns to numeric (1-4) in one pass
        df = self.df_full_joined
        for col_name in categorical_columns:
            df = df.withColumn(f"{col_name}_numeric", category_to_numeric(col_name))

        # --- Reliability score (weighted average of 7 components, equal weights) ---
        reliability_cols = [
            "ip_change_category_numeric", "steer_start_category_numeric",
            "steer_start_perHome_category_numeric", "customer_reboot_category_numeric",
            "reboot_category_numeric", "modem_reset_category_numeric",
            "sudden_drop_category_numeric",
        ]
        # Native Spark expression: ignore nulls by summing weight only for non-null cols
        reliability_expr = (
            F.round(
                F.nanvl(
                    reduce(lambda a, b: a + b, [F.coalesce(F.col(c), F.lit(0)) for c in reliability_cols])
                    / reduce(lambda a, b: a + b, [F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in reliability_cols]),
                    F.lit(None).cast("float"),
                ),
                2,
            )
        )

        # --- Speed score (equal weight: phyrate + airtime) ---
        speed_cols = ["phyrate_category_numeric", "Airtime_Utilization_Category_numeric"]
        speed_expr = F.round(
            (F.coalesce(F.col(speed_cols[0]), F.lit(0)) + F.coalesce(F.col(speed_cols[1]), F.lit(0)))
            / F.when(F.col(speed_cols[0]).isNotNull() | F.col(speed_cols[1]).isNotNull(),
                     F.when(F.col(speed_cols[0]).isNotNull(), 1).otherwise(0)
                     + F.when(F.col(speed_cols[1]).isNotNull(), 1).otherwise(0))
             .otherwise(None),
            2,
        )

        df = (
            df.withColumn("reliabilityScore", reliability_expr)
              .withColumn("speedScore",       speed_expr)
              .withColumn("coverageScore",    F.col("rssi_numeric"))
        )

        # Convert composite scores back to categories
        for score_col in ["reliabilityScore", "speedScore", "coverageScore"]:
            df = df.withColumn(f"numerical_{score_col}", F.col(score_col).cast("double"))
            df = df.withColumn(score_col, numeric_to_category(score_col))

        # Overall wifi score = worst of the three sub-scores (native Spark, no UDF)
        score_priority = F.create_map(
            F.lit("Poor"),      F.lit(1),
            F.lit("Fair"),      F.lit(2),
            F.lit("Good"),      F.lit(3),
            F.lit("Excellent"), F.lit(4),
        )
        df = df.withColumn(
            "wifiScore",
            F.array_min(F.array(
                score_priority[F.col("reliabilityScore")],
                score_priority[F.col("speedScore")],
                score_priority[F.col("coverageScore")],
            ))
        )
        # Map the min numeric value back to the label
        df = df.withColumn(
            "wifiScore",
            F.when(F.col("wifiScore") == 1, "Poor")
             .when(F.col("wifiScore") == 2, "Fair")
             .when(F.col("wifiScore") == 3, "Good")
             .when(F.col("wifiScore") == 4, "Excellent")
             .otherwise(None),
        )

        (
            df.repartition(200)
              .filter(F.col("sn") != "G402121101548133")
              .write.mode("overwrite").parquet(self.wifiscore_path)
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def process_kpi_data(date_list: list, email_sender):
    for date_val in date_list:
        file_date = date_val.strftime("%Y-%m-%d")
        output_path = f"{HDFS_PD}/user/ZheS/wifi_score_v4/wifiScore_location/{file_date}"

        if path_exists(f"{output_path}/_SUCCESS"):
            print(f"Data for {file_date} already exists, skipping.")
            continue

        try:
            date_compact = file_date.replace("-", "")

            env_configs = {
                "crsp": {
                    "owl_path":        f"{HDFS_PA}/sha_data/OWLHistory/date={date_compact}",
                    "station_history": f"{HDFS_PA}/sha_data/StationHistory/date={date_compact}",
                    "deviceGroup":     f"{HDFS_PA}/sha_data/DeviceGroups/date={date_compact}",
                    "df_rssi":         f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/crsp_df_rowkey_rssi_category/{file_date}",
                    "df_phyrate":      f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/crsp_df_rowkey_phyrate_category/{file_date}",
                    "wifiscore":       f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/crsp_wifiscore/{file_date}",
                },
                "pac": {
                    "owl_path":        f"{HDFS_PA}/sha_data/purple_prod/bhrx_owlhistory/date={date_compact}",
                    "station_history": f"{HDFS_PA}/sha_data/purple_prod/bhrx_stationhistory/date={date_compact}",
                    "deviceGroup":     f"{HDFS_PA}/sha_data/purple_prod/bhrx_devicegroups/date={date_compact}",
                    "df_rssi":         f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/pac_df_rowkey_rssi_category/{file_date}",
                    "df_phyrate":      f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/pac_df_rowkey_phyrate_category/{file_date}",
                    "wifiscore":       f"{HDFS_PD}/user/ZheS/wifi_score_v4/backup/pac_wifiscore/{file_date}",
                },
                "all": {
                    "df_rssi":    f"{HDFS_PD}/user/ZheS/wifi_score_v4/df_rowkey_rssi_category/{file_date}",
                    "df_phyrate": f"{HDFS_PD}/user/ZheS/wifi_score_v4/df_rowkey_phyrate_category/{file_date}",
                    "wifiscore":  f"{HDFS_PD}/user/ZheS/wifi_score_v4/KPI/{file_date}",
                },
            }

            # Process each environment independently
            for env in ["crsp", "pac"]:
                try:
                    cfg      = env_configs[env]
                    analysis = WifiKPIAnalysis(
                        date_val             = date_val,
                        owl_path             = cfg["owl_path"],
                        station_history_path = cfg["station_history"],
                        deviceGroup_path     = cfg["deviceGroup"],
                        df_rssi_path         = cfg["df_rssi"],
                        df_phyrate_path      = cfg["df_phyrate"],
                        wifiscore_path       = cfg["wifiscore"],
                    )
                    analysis.get_score()
                    print(f"[OK] {env} wifiscore written to {cfg['wifiscore']}")
                except Exception:
                    print(f"[ERROR] {env} {file_date}\n{traceback.format_exc()}")

            # Merge crsp + pac outputs into combined "all" outputs
            for key in ["df_rssi", "df_phyrate", "wifiscore"]:
                merge_key = "rowkey" if key != "wifiscore" else "sn"
                try:
                    base_df   = spark.read.parquet(env_configs["crsp"][key])
                    update_df = spark.read.parquet(env_configs["pac"][key])
                    replace_rows(base_df, update_df, merge_key)\
                        .write.mode("overwrite").parquet(env_configs["all"][key])
                except Exception:
                    # Fall back to crsp only if pac is unavailable
                    spark.read.parquet(env_configs["crsp"][key])\
                         .write.mode("overwrite").parquet(env_configs["all"][key])

            # Attach location data
            location_df = (
                spark.read.option("recursiveFileLookup", "true")
                     .parquet(HDFS_PD + "/user/ZheS/wifi_score_v4/County_location")
                     .select("sn", "mdn", "state", "county", "latitude", "longitude")
                     .distinct()
                     .drop("wifiScore")
            )
            (
                spark.read.parquet(env_configs["all"]["wifiscore"])
                     .withColumn("rowkey", F.concat(F.substring("sn", -4, 4), F.lit("-"), F.col("sn")))
                     .join(location_df, on="sn", how="left")
                     .write.mode("overwrite")
                     .parquet(f"{HDFS_PD}/user/ZheS/wifi_score_v4/wifiScore_location/{file_date}")
            )

            # Filter to supported models and export to AWS path
            supported_models = [
                "ASK-NCQ1338", "ASK-NCQ1338FA", "XCI55AX", "WNC-CR200A",
                "ASK-NCM1100", "CR1000A", "CR1000B", "G3100", "CHR30A-R",
            ]
            (
                spark.read.parquet(env_configs["all"]["wifiscore"])
                     .filter(F.col("model_name").isin(supported_models))
                     .drop("firmware")
                     .write.mode("overwrite")
                     .parquet(f"{HDFS_PD}/user/ZheS/wifi_score_v4/aws/{file_date}")
            )

        except Exception:
            error_message = f"wifiScore v4 failed at {file_date}\n\n{traceback.format_exc()}"
            print(error_message)
            email_sender.send(
                send_from="wifiKPIAnalysis@verizon.com",
                subject  =f"wifiKPIAnalysis failed at {file_date}",
                text     =error_message,
                cc       =["injure21@gmail.com"],
            )


if __name__ == "__main__":
    spark = (
        SparkSession.builder
                    .appName("Zhe_wifi_score")
                    .config("spark.ui.port", "24045")
                    .getOrCreate()
    )
    email_sender = MailSender()

    parser = argparse.ArgumentParser(description="WiFi Score KPI pipeline")
    parser.add_argument(
        "--date",
        default=(datetime.now(timezone.utc) - timedelta(1)).strftime("%Y-%m-%d"),
    )
    args_date = parser.parse_args().date

    backfill_range = 10
    date_list = [
        (datetime.strptime(args_date, "%Y-%m-%d") - timedelta(days=i)).date()
        for i in range(backfill_range)
    ][::-1]

    process_kpi_data(date_list, email_sender)
