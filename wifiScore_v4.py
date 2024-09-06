from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 

from pyspark.sql.functions import from_unixtime 
import argparse 
import pandas as pd
from functools import reduce
from pyspark.sql import DataFrame

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import *
from pyspark.sql import Row
from functools import reduce
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F, Window
from datetime import timedelta

class wifiKPIAnalysis:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self, date_val):
        self.date_val = date_val
        self.owl_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{ date_val.strftime('%Y%m%d')  }"
        self.station_history_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{ date_val.strftime('%Y%m%d')  }"

        self.load_data()

    def load_data(self):

        self.df_owl = spark.read.parquet( self.owl_path )\
            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
        
        self.df_sh = spark.read.parquet(self.station_history_path)\
                    .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                    .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))


    def calculate_reboot(self, df_owl = None):
        """Calculate the number of reboots for each serial number (sn)."""
        if df_owl is None:
            df_owl = self.df_owl

        self.df_restart = df_owl.select("sn", "ts", "Diag_Result_dev_restart")\
                                .filter(F.col("Diag_Result_dev_restart").isNotNull())\
                                .groupby("sn")\
                                .agg(F.count("*").alias("no_reboot"))

    def calculate_ip_changes(self, df_owl = None, df_restart = None):
        
        if df_owl is None:
            df_owl = self.df_owl
        if df_restart is None:
            df_restart = self.df_restart
        # Calculate the number of IP changes for each serial number (sn)
        window_spec = Window.partitionBy("model_name", "sn").orderBy("ts")

        ip_change_df = (
            df_owl.filter(F.col("owl_data_fwa_cpe_data").isNotNull())\
                        .withColumn("model_name", F.get_json_object(F.col("Owl_Data_fwa_cpe_data"), "$.ModelName"))\
                        .withColumn("ipv4_ip", F.get_json_object(F.col("Owl_Data_fwa_cpe_data"), "$.ipv4_ip"))\
                        .filter(F.col("ipv4_ip").isNotNull())\
                        .withColumn("prev_ip4", F.lag("ipv4_ip").over(window_spec))\
                        .withColumn("ip_changes_flag",
                                    F.when((F.col("ipv4_ip") != F.col("prev_ip4")) & F.col("prev_ip4").isNotNull(), 1).otherwise(0))\
                        .groupby("sn", "model_name")\
                        .agg(F.sum("ip_changes_flag").alias("no_ip_changes"))
            )
        # Join ip_change_df with df_restart and categorize IP changes performance.
        self.ip_change_df = (
            ip_change_df.join(df_restart, on="sn", how="left")\
                            .withColumn("reboot", F.when(F.col("no_reboot").isNotNull(), "y").otherwise("n"))\
                            .withColumn(
                                        "ip_change_rating",
                                        F.when((F.col("reboot") == "n") & (F.col("no_ip_changes") > 6), "Poor")
                                        .when((F.col("reboot") == "n") & (F.col("no_ip_changes").between(4, 6)), "Fair")
                                        .when((F.col("reboot") == "n") & (F.col("no_ip_changes").between(1, 3)), "Good")
                                        .when((F.col("reboot") == "n") & (F.col("no_ip_changes") == 0), "Excellent")
                                        .when((F.col("reboot") == "y") & (F.col("no_ip_changes") >= 16), "Poor")
                                        .when((F.col("reboot") == "y") & (F.col("no_ip_changes").between(7, 15)), "Fair")
                                        .when((F.col("reboot") == "y") & (F.col("no_ip_changes").between(1, 6)), "Good")
                                        .when((F.col("reboot") == "y") & (F.col("no_ip_changes") == 0), "Excellent")
                                        .otherwise("Unknown")
                                        )
                            )

    def calculate_steer(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
        
        # calculate band steer
        df_bandsteer = df_sh.select( "sn","rowkey", "ts",
                                    col("Diag_Result_band_steer.sta_type").alias("sta_type"),
                                    col("Diag_Result_band_steer.orig.name").alias("orig_name"),
                                    col("diag_result_band_steer.action").alias("action"),
                                    col("Diag_Result_band_steer.intend.band").alias("intend_band"),
                                    col("Diag_Result_band_steer.orig.band").alias("orig_band"),
                                    col("Diag_Result_band_steer.target.band").alias("target_band")
                                    )\
                            .withColumn( "intend_band", when( col("intend_band") == "5G_H", "5G").otherwise( col("intend_band") ) )\
                            .withColumn( "orig_band", when( col("orig_band") == "5G_H", "5G").otherwise( col("orig_band") ) )\
                            .withColumn( "target_band", when( col("target_band") == "5G_H", "5G").otherwise( col("target_band") ) )\
                            .filter(  ( F.col("sta_type") == "2" )  )
        
        df_bandsteer = df_bandsteer.groupBy("sn")\
                                        .agg(
                                            count(when(col("action") == "1", True)).alias("band_success_count"),
                                            count("*").alias("band_total_count")
                                        )\
                                        .withColumn(
                                                    "band_success_percentage",
                                                    (col("band_success_count") / col("band_total_count")) * 100
                                                )
        # calculate ap steer
        df_apsteer = df_sh.select( "sn","rowkey", "ts",
                                    col("Diag_Result_ap_steer.sta_type").alias("sta_type"),
                                    col("Diag_Result_ap_steer.orig.name").alias("orig_name"),
                                    col("Diag_Result_ap_steer.action").alias("action"),
                                    col("Diag_Result_ap_steer.intend.band").alias("intend_band"),
                                    col("Diag_Result_ap_steer.orig.band").alias("orig_band"),
                                    col("Diag_Result_ap_steer.target.band").alias("target_band")
                                    )\
                            .withColumn( "intend_band", when( col("intend_band") == "5G_H", "5G").otherwise( col("intend_band") ) )\
                            .withColumn( "orig_band", when( col("orig_band") == "5G_H", "5G").otherwise( col("orig_band") ) )\
                            .withColumn( "target_band", when( col("target_band") == "5G_H", "5G").otherwise( col("target_band") ) )\
                            .filter(  ( F.col("sta_type") == "2" )  )
        
        df_apsteer = df_apsteer.groupBy("sn")\
                                        .agg(
                                            count(when(col("action") == "1", True)).alias("ap_success_count"),
                                            count("*").alias("ap_total_count")
                                        )\
                                        .withColumn(
                                                    "ap_success_percentage",
                                                    (col("ap_success_count") / col("ap_total_count")) * 100
                                                )

        self.son_df = df_bandsteer.join(df_apsteer, on="sn", how="full_outer")\
                                    .withColumn(
                                        "success_count",
                                        F.coalesce(col("band_success_count"), lit(0)) + F.coalesce(col("ap_success_count"), lit(0))
                                    ).withColumn(
                                        "total_count",
                                        F.coalesce(col("band_total_count"), lit(0)) + F.coalesce(col("ap_total_count"), lit(0))
                                    )

    def calculate_reboot(self, df_owl = None):
        
        if df_owl is None:
            df_owl = self.df_owl
        if df_restart is None:
            df_restart = self.df_restart
        # Number of reboots per home
        def categorize_reboots_per_home(column):
            return F.when(F.col(column) >= 5, "Poor")\
                    .when(F.col(column) == 4, "Fair")\
                    .when((F.col(column) >= 2) & (F.col(column) <= 3), "Good")\
                    .when(F.col(column) >= 1, "Excellent")\
                    .otherwise(None)

        # Number of modem resets per home
        def categorize_modem_resets_per_home(column):
            return F.when(F.col(column) >= 5, "Poor")\
                    .when(F.col(column) == 4, "Fair")\
                    .when((F.col(column) >= 2) & (F.col(column) <= 3), "Good")\
                    .when(F.col(column) >= 1, "Excellent")\
                    .otherwise(None)

        # Number of reboots initiated via customer
        def categorize_customer_initiated_reboots(column):
            return F.when(F.col(column) >= 3, "Poor")\
                    .when(F.col(column) == 2, "Fair")\
                    .when(F.col(column) == 1, "Good")\
                    .when(F.col(column) == 0, "Excellent")\
                    .otherwise(None)
        
        df_modem_crash = df_owl.filter( (F.col("owl_data_modem_event").isNotNull()) )\
                                .groupBy("sn") \
                                .agg(F.count("*").alias("num_mdm_crashes"))

        df_user_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .filter(col("reason").isin(["GUI","APP","BTN"]) )\
                            .groupby("sn")\
                            .agg( F.count("*").alias("num_user_reboots"))

        df_not_user_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .filter(~col("reason").isin(["GUI","APP","BTN"]) )\
                            .groupby("sn")\
                            .agg( F.count("*").alias("num_not_user_reboots"))

        # Step 3: Applying the categorization for each reboot/reset column
        df_rbt = df_modem_crash.join(df_user_rbt, on="sn", how="full_outer").join(df_not_user_rbt, on="sn", how="full_outer")\
                    .withColumn("reboot_category", categorize_reboots_per_home("num_not_user_reboots"))\
                    .withColumn("modem_reset_category", categorize_modem_resets_per_home("num_mdm_crashes"))\
                    .withColumn("customer_reboot_category", categorize_customer_initiated_reboots("num_user_reboots"))
        
        return df_rbt

    def save_results(self):

        ip_change_df_path = f"{hdfs_pd}/user/ZheS/wifi_score_v4/ip_change_df/{(self.date_val - timedelta(1)).strftime('%Y-%m-%d')}"
        #self.ip_change_df.write.mode("overwrite").parquet(ip_change_df_path)
        son_df_path = f"{hdfs_pd}/user/ZheS/wifi_score_v4/son_df/{(self.date_val - timedelta(1)).strftime('%Y-%m-%d')}"
        self.son_df.write.mode("overwrite").parquet(son_df_path)
        son_df_path = f"{hdfs_pd}/user/ZheS/wifi_score_v4/son_df/{(self.date_val - timedelta(1)).strftime('%Y-%m-%d')}"
        

    def run(self):

        self.load_data()
        #self.calculate_reboot()
        #self.calculate_ip_changes()
        self.calculate_steer()
        self.df_rbt = self.calculate_reboot()
        self.save_results()


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    parser = argparse.ArgumentParser(description="Inputs")
    day_before = 1
    date_val = ( date.today() - timedelta(day_before) )

    analysis = wifiKPIAnalysis(  date_val = date_val)
    analysis.run()





