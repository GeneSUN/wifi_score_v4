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

sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender

def read_file_by_date(date, file_path_pattern): 
    try:
        file_path = file_path_pattern.format(date) 
        df = spark.read.parquet(file_path) 
        return df 
    except:
        return None

def process_parquet_files_for_date_range(date_range, file_path_pattern): 

    df_list = list(map(lambda date: read_file_by_date(date, file_path_pattern), date_range)) 
    df_list = list(filter(None, df_list)) 
    result_df = reduce(lambda df1, df2: df1.union(df2), df_list) 

    return result_df 

class wifiKPIAnalysis:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self, date_val):
        self.date_val = date_val
        self.owl_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{ (date_val+timedelta(1)).strftime('%Y%m%d')  }"
        self.station_history_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{ (date_val+timedelta(1)).strftime('%Y%m%d')  }"
        self.deviceGroup_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/{ (date_val+timedelta(1)).strftime('%Y%m%d')  }"

        self.load_data()

    def load_data(self):
        
        self.df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .select("rowkey",explode("Group_Data_sys_info"))\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                            .select("sn",col("col.model").alias("model_name") )

        self.df_owl = spark.read.parquet( self.owl_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
        
        self.df_sh = spark.read.parquet(self.station_history_path)\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))

    def calculate_ip_changes(self, df_owl = None, df_restart = None, ip_change_file_path_template = None):
        
        if df_owl is None:
            df_owl = self.df_owl
        if ip_change_file_path_template is None:
            ip_change_file_path_template = hdfs_pd + "/user/ZheS/wifi_score_v4/time_window/{}/ip_change_daily_df/"

        lookback_days = 4
        date_range_list = [(self.date_val - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_ip_change_df = process_parquet_files_for_date_range(date_range_list, ip_change_file_path_template)
        historical_ip_change_df = historical_ip_change_df.groupby("sn")\
                                                        .agg(F.sum("no_ip_changes").alias("no_ip_changes"))

        ip_change_cat_df = (
            historical_ip_change_df.withColumn(
                                            "ip_change_rating",
                                            F.when(F.col("no_ip_changes") > 6, "Poor")
                                            .when(F.col("no_ip_changes").between(4, 6), "Fair")
                                            .when(F.col("no_ip_changes").between(1, 3), "Good")
                                            .when(F.col("no_ip_changes") == 0, "Excellent")
                                            .otherwise("Unknown")
                                        )
                        )

        return ip_change_cat_df

    def calculate_airtime(self, ip_change_file_path_template = None):
        
        if ip_change_file_path_template is None:
            ip_change_file_path_template = hdfs_pd + "/user/ZheS/wifi_score_v4/time_window/{}/airtime_daily_df/"

        lookback_days = 7
        date_range_list = [(self.date_val - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_airtime_df = process_parquet_files_for_date_range(date_range_list, ip_change_file_path_template)
        historical_airtime_df = historical_airtime_df.groupby("sn")\
                                                        .agg(F.sum("sum_airtime_util").alias("sum_airtime_util"))

        airtime_df = (
            historical_airtime_df.withColumn(
                                                "Airtime_Utilization_Category",
                                                F.when(F.col("sum_airtime_util") > 200, "Poor") 
                                                 .when((F.col("sum_airtime_util") >= 75) & (F.col("sum_airtime_util") <= 200), "Fair") 
                                                 .when((F.col("sum_airtime_util") >= 26) & (F.col("sum_airtime_util") <= 74), "Good")  
                                                 .when(F.col("sum_airtime_util") >= 0, "Excellent") 
                                                 .otherwise("Unknown")  
                                            )
                        )

        return airtime_df

    def calculate_steer(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
        
        df_bandsteer = df_sh.select( "sn",
                                    col("Diag_Result_band_steer.sta_type").alias("sta_type"),
                                    col("Diag_Result_band_steer.action").alias("action"),
                                    )\
                            .filter(  ( F.col("sta_type") == "2" )  )
        
        df_bandsteer = df_bandsteer.groupBy("sn")\
                                    .agg(
                                        count(  when(  (col("action") == "1")&( F.col("sta_type") == "2" ) , True)).alias("band_success_count"),
                                        count(  when(  (col("action") == "0")&( F.col("sta_type") == "2" ) , True)).alias("band_failure_count"),
                                        count(  when(  ( F.col("sta_type") == "2" ) , True)).alias("band_total_count"),
                                    )\
                                    .withColumn(
                                                "band_success_percentage",
                                                (col("band_success_count") / col("band_total_count")) * 100
                                    )


        df_apsteer = df_sh.select( "sn",
                                    col("Diag_Result_ap_steer.sta_type").alias("sta_type"),
                                    col("Diag_Result_ap_steer.action").alias("action"),
                                    )\
                            .filter(  ( F.col("sta_type") == "2" )  )
        
        df_apsteer = df_apsteer.groupBy("sn")\
                                .agg(
                                    count(  when(  (col("action") == "1")&( F.col("sta_type") == "2" ) , True)).alias("ap_success_count"),
                                    count(  when(  (col("action") == "0")&( F.col("sta_type") == "2" ) , True)).alias("ap_failure_count"),
                                    count(  when(  ( F.col("sta_type") == "2" ) , True)).alias("ap_total_count")
                                )\
                                .withColumn(
                                            "ap_success_percentage",
                                            (col("ap_success_count") / col("ap_total_count")) * 100
                                        )

        son_df = df_bandsteer.join(df_apsteer, on="sn", how="full_outer")\
                                    .withColumn(
                                        "success_count",
                                        F.coalesce(col("band_success_count"), lit(0)) + F.coalesce(col("ap_success_count"), lit(0))
                                    )\
                                    .withColumn(
                                        "failure_count",
                                        F.coalesce(col("band_failure_count"), lit(0)) + F.coalesce(col("ap_failure_count"), lit(0))
                                    )\
                                    .withColumn(
                                        "total_count",
                                        F.coalesce(col("band_total_count"), lit(0)) + F.coalesce(col("ap_total_count"), lit(0))
                                    )\
                            .withColumn(
                                        "SON_steer_start",
                                        F.when(F.col("success_count") > 60, "Poor")
                                        .when((F.col("success_count") >= 31) & (F.col("success_count") <= 60), "Fair")
                                        .when((F.col("success_count") >= 11) & (F.col("success_count") <= 29), "Good")
                                        .when((F.col("success_count") >= 1) & (F.col("success_count") <= 10), "Excellent")
                                        .otherwise("Unknown")
                                    )

        return son_df

    def calculate_restart(self, df_owl = None):
        
        if df_owl is None:
            df_owl = self.df_owl

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
        
        restart_daily_file_path_template = hdfs_pd + "/user/ZheS/wifi_score_v4/time_window/{}/restart_daily_df"
        previous_day = self.date_val
        lookback_days = 4
        date_range_list = [(previous_day - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_restart_daily_df = process_parquet_files_for_date_range(date_range_list, restart_daily_file_path_template)
        historical_restart_daily_df.groupby("sn")\
                                    .agg( F.sum("num_mdm_crashes").alias("num_mdm_crashes"),
                                            F.sum("num_user_reboots").alias("num_user_reboots"),
                                            F.sum("num_total_reboots").alias("num_total_reboots"),
                                )\

        df_start = historical_restart_daily_df\
                        .withColumn("reboot_category", categorize_reboots_per_home("num_total_reboots"))\
                        .withColumn("modem_reset_category", categorize_modem_resets_per_home("num_mdm_crashes"))\
                        .withColumn("customer_reboot_category", categorize_customer_initiated_reboots("num_user_reboots"))
        
        return df_start

    def calculate_rssi(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh

        df_flattened = df_sh.withColumn("connect_type", F.col("Station_Data_connect_data.connect_type"))\
                            .withColumn(
                                        "connect_type",
                                        F.when(F.col("connect_type").like("2.4G%"), "2_4G")
                                        .when(F.col("connect_type").like("5G%"), "5G")
                                        .when(F.col("connect_type").like("6G%"), "6G")
                                        .otherwise(F.col("connect_type"))  
                                    )\
                            .withColumn("signal_strength", F.col("Station_Data_connect_data.signal_strength"))\
                            .withColumn("airtime_util", F.col("Station_Data_connect_data.airtime_util"))\
                            .filter( col("signal_strength").isNotNull() )\
                            .withColumn("signal_strength_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("signal_strength")))\
                            .withColumn("signal_strength_5GHz",  F.when(F.col("connect_type") == "5G", F.col("signal_strength")))\
                            .withColumn("signal_strength_6GHz", F.when(F.col("connect_type") == "6G", F.col("signal_strength")) )\
                            .select("sn","rowkey","ts","connect_type","signal_strength", "signal_strength_2_4GHz","signal_strength_5GHz","signal_strength_6GHz",)

        window_spec = Window.partitionBy("sn")

        df_grouped = df_flattened.groupBy("sn", "rowkey")\
                                    .agg(
                                        F.count("signal_strength").alias("sample_count"),
                                        
                                        F.round(F.avg("signal_strength_2_4GHz"), 2).alias("avg_signal_strength_2_4GHz"),
                                        F.round(F.avg("signal_strength_5GHz"), 2).alias("avg_signal_strength_5GHz"),
                                        F.round(F.avg("signal_strength_6GHz"), 2).alias("avg_signal_strength_6GHz"),
                                        
                                        F.sum(F.when(F.col("connect_type") == "2_4G", 1).otherwise(0)).alias("rssi_count_2_4GHz"),
                                        F.sum(F.when(F.col("connect_type") == "5G", 1).otherwise(0)).alias("rssi_count_5GHz"),
                                        F.sum(F.when(F.col("connect_type") == "6G", 1).otherwise(0)).alias("rssi_count_6GHz")
                                    )\
                                    .filter(F.col("sample_count") >= 12)\
                                    .withColumn("distinct_rowkey_count", F.count("rowkey").over(window_spec))

        df_classified = df_grouped.withColumn(
                                            "RSSI_category_2_4GHz",
                                            F.when(F.col("avg_signal_strength_2_4GHz") < -78, "Poor")
                                            .when((F.col("avg_signal_strength_2_4GHz") >= -77) & (F.col("avg_signal_strength_2_4GHz") <= -71), "Fair")
                                            .when((F.col("avg_signal_strength_2_4GHz") >= -70) & (F.col("avg_signal_strength_2_4GHz") <= -56), "Good")
                                            .when(F.col("avg_signal_strength_2_4GHz") > -55, "Excellent")
                                            .otherwise("No Data")
                                ).withColumn(
                                            "RSSI_category_5GHz",
                                            F.when(F.col("avg_signal_strength_5GHz") < -75, "Poor")
                                            .when((F.col("avg_signal_strength_5GHz") >= -75) & (F.col("avg_signal_strength_5GHz") <= -71), "Fair")
                                            .when((F.col("avg_signal_strength_5GHz") >= -70) & (F.col("avg_signal_strength_5GHz") <= -56), "Good")
                                            .when(F.col("avg_signal_strength_5GHz") > -55, "Excellent")
                                            .otherwise("No Data")
                                ).withColumn(
                                            "RSSI_category_6GHz",
                                            F.when(F.col("avg_signal_strength_6GHz") < -70, "Poor")
                                            .when((F.col("avg_signal_strength_6GHz") >= -70) & (F.col("avg_signal_strength_6GHz") <= -65), "Fair")
                                            .when((F.col("avg_signal_strength_6GHz") >= -65) & (F.col("avg_signal_strength_6GHz") <= -56), "Good")
                                            .when(F.col("avg_signal_strength_6GHz") > -55, "Excellent")
                                            .otherwise("No Data")
                                        )
      
                                        
        bands = ["2_4GHz", "5GHz", "6GHz"]
        categories = ["Poor", "Fair", "Good", "Excellent"]
        for band in bands:
            for category in categories:
                df_classified = df_classified.withColumn(
                                f"is_{category.lower()}_{band}",
                                F.when((F.col(f"RSSI_category_{band}") == category) & (F.col(f"rssi_count_{band}") >= 12), 1).otherwise(0)
                            )
        df_classified = df_classified.withColumn(
                                            "final_RSSI_category",
                                            F.when((F.col("is_poor_2_4GHz") == 1) | (F.col("is_poor_5GHz") == 1) , "Poor")
                                            .when((F.col("is_fair_2_4GHz") == 1) | (F.col("is_fair_5GHz") == 1) , "Fair")
                                            .when((F.col("is_good_2_4GHz") == 1) | (F.col("is_good_5GHz") == 1) , "Good")
                                            .when((F.col("is_excellent_2_4GHz") == 1) | (F.col("is_excellent_5GHz") == 1) , "Excellent")
                                            .otherwise("No Data")
                                        )

        df_with_numeric_rssi = df_classified.withColumn(
                                                        "numeric_RSSI_category",
                                                        F.when(F.col("final_RSSI_category") == "Poor", 1)
                                                        .when(F.col("final_RSSI_category") == "Fair", 2)
                                                        .when(F.col("final_RSSI_category") == "Good", 3)
                                                        .when(F.col("final_RSSI_category") == "Excellent", 4)
                                                        )
        df_final_rssi_category = df_with_numeric_rssi.groupBy("sn")\
                                                    .agg(F.avg("numeric_RSSI_category").alias("avg_RSSI_score"))\
                                                    .withColumn(
                                                                "final_RSSI_category",
                                                                F.when(F.col("avg_RSSI_score") <= 1.5, "Poor")
                                                                .when((F.col("avg_RSSI_score") > 1.5) & (F.col("avg_RSSI_score") <= 2.5), "Fair")
                                                                .when((F.col("avg_RSSI_score") > 2.5) & (F.col("avg_RSSI_score") <= 3.5), "Good")
                                                                .when(F.col("avg_RSSI_score") > 3.5, "Excellent")
                                                            )
        return df_final_rssi_category
    
    def calculate_phyrate(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
            
        df_phyrate = df_sh.withColumn("connect_type", F.col("Station_Data_connect_data.connect_type"))\
                            .withColumn(
                                        "connect_type",
                                        F.when(F.col("connect_type").like("2.4G%"), "2_4G")
                                        .when(F.col("connect_type").like("5G%"), "5G")
                                        .when(F.col("connect_type").like("6G%"), "6G")
                                        .otherwise(F.col("connect_type"))  # If it doesn't match, leave it as is
                                    )\
                            .filter( col("connect_type").isin( ["2_4G","5G","6G"]) )\
                            .withColumn("tx_link_rate", F.col("Station_Data_connect_data.tx_link_rate"))\
                            .withColumn("tx_link_rate", F.regexp_replace(F.col("tx_link_rate"), "Mbps", "") )\
                            .withColumn("tx_link_rate_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("tx_link_rate")))\
                            .withColumn("tx_link_rate_5GHz",  F.when(F.col("connect_type") == "5G", F.col("tx_link_rate")))\
                            .withColumn("tx_link_rate_6GHz", F.when(F.col("connect_type") == "6G", F.col("tx_link_rate")) )\
                            .select("sn","rowkey","ts","connect_type","tx_link_rate", "tx_link_rate_2_4GHz","tx_link_rate_5GHz","tx_link_rate_6GHz")

        window_spec = Window.partitionBy("sn")

        df_grouped = df_phyrate.groupBy("sn", "rowkey")\
                                .agg(
                                    F.count("*").alias("phyrate_count"),
                                    
                                    F.round(F.avg("tx_link_rate_2_4GHz"), 2).alias("avg_tx_link_rate_2_4GHz"),
                                    F.round(F.avg("tx_link_rate_5GHz"), 2).alias("avg_tx_link_rate_5GHz"),
                                    F.round(F.avg("tx_link_rate_6GHz"), 2).alias("avg_tx_link_rate_6GHz"),
                                    
                                    F.sum(F.when(F.col("connect_type") == "2_4G", 1).otherwise(0)).alias("phyrate_count_2_4GHz"),
                                    F.sum(F.when(F.col("connect_type") == "5G", 1).otherwise(0)).alias("phyrate_count_5GHz"),
                                    F.sum(F.when(F.col("connect_type") == "6G", 1).otherwise(0)).alias("phyrate_count_6GHz")
                                )\
                                .filter(F.col("phyrate_count") >= 12)\
                                .withColumn("distinct_rowkey_count", F.count("rowkey").over(window_spec))
        
        df_classified = df_grouped.withColumn(
                                    "tx_category_2_4GHz",
                                    F.when(F.col("avg_tx_link_rate_2_4GHz") < 80, "Poor")
                                    .when((F.col("avg_tx_link_rate_2_4GHz") >= 80) & (F.col("avg_tx_link_rate_2_4GHz") <= 100), "Fair")
                                    .when((F.col("avg_tx_link_rate_2_4GHz") >= 101) & (F.col("avg_tx_link_rate_2_4GHz") <= 120), "Good")
                                    .when(F.col("avg_tx_link_rate_2_4GHz") > 120, "Excellent")
                                    .otherwise("No Data")  
                                )\
                                .withColumn(
                                    "tx_category_5GHz",
                                    F.when(F.col("avg_tx_link_rate_5GHz") < 200, "Poor")
                                    .when((F.col("avg_tx_link_rate_5GHz") >= 200) & (F.col("avg_tx_link_rate_5GHz") <= 350), "Fair")
                                    .when((F.col("avg_tx_link_rate_5GHz") >= 351) & (F.col("avg_tx_link_rate_5GHz") <= 500), "Good")
                                    .when(F.col("avg_tx_link_rate_5GHz") > 500, "Excellent")
                                    .otherwise("No Data")
                                )

        bands = ["2_4GHz", "5GHz"]
        categories = ["Poor", "Fair", "Good", "Excellent"]

        for band in bands:
            for category in categories:
                df_classified = df_classified.withColumn(
                    f"is_{category.lower()}_{band}",
                    F.when((F.col(f"tx_category_{band}") == category) & (F.col(f"phyrate_count_{band}") >= 12), 1).otherwise(0)
                )

        df_classified = df_classified.withColumn(
                                            "final_phyrate_category",
                                            F.when((F.col("is_poor_2_4GHz") == 1) | (F.col("is_poor_5GHz") == 1) , "Poor")
                                            .when((F.col("is_fair_2_4GHz") == 1) | (F.col("is_fair_5GHz") == 1) , "Fair")
                                            .when((F.col("is_good_2_4GHz") == 1) | (F.col("is_good_5GHz") == 1) , "Good")
                                            .when((F.col("is_excellent_2_4GHz") == 1) | (F.col("is_excellent_5GHz") == 1) , "Excellent")
                                            .otherwise("No Data")
                                        )

        df_with_numeric_phyrate = df_classified.withColumn(
                                                            "numeric_phyrate_category",
                                                            F.when(F.col("final_phyrate_category") == "Poor", 1)
                                                            .when(F.col("final_phyrate_category") == "Fair", 2)
                                                            .when(F.col("final_phyrate_category") == "Good", 3)
                                                            .when(F.col("final_phyrate_category") == "Excellent", 4)
                                                        )


        df_final_phyrate_category = df_with_numeric_phyrate.groupBy("sn")\
                                                            .agg(F.avg("numeric_phyrate_category").alias("avg_phyrate_score"))\
                                                            .withColumn(
                                                                "final_phyrate_category",
                                                                F.when(F.col("avg_phyrate_score") <= 1.5, "Poor")
                                                                .when((F.col("avg_phyrate_score") > 1.5) & (F.col("avg_phyrate_score") <= 2.5), "Fair")
                                                                .when((F.col("avg_phyrate_score") > 2.5) & (F.col("avg_phyrate_score") <= 3.5), "Good")
                                                                .when(F.col("avg_phyrate_score") > 3.5, "Excellent")
                                                            )

        return df_final_phyrate_category

    def run(self):

        self.load_data()
        ip_change_cat_df = self.calculate_ip_changes()
        son_df = self.calculate_steer()
        df_restart = self.calculate_restart()
        df_rssi = self.calculate_rssi()
        df_phyrate = self.calculate_phyrate()
        df_airtime = self.calculate_airtime()

        df_full_joined = ip_change_cat_df.join(son_df, on="sn", how="full_outer")\
                                        .join(df_restart, on="sn", how="full_outer")\
                                        .join(df_rssi, on="sn", how="full_outer")\
                                        .join(df_phyrate, on="sn", how="full_outer")\
                                        .join(df_airtime, on="sn", how="full_outer")\
                                        .join(self.df_dg, on="sn", how="inner")\
                                        .distinct()

        df_full_joined.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/KPI/{(self.date_val).strftime('%Y-%m-%d')}")
        

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    email_sender = MailSender()
    
    backfill_range = 4
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=(date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args_date = parser.parse_args().date
    date_list = [( datetime.strptime( args_date, "%Y-%m-%d" )  - timedelta(days=i)).date() for i in range(backfill_range)][::-1]

    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    def process_kpi_data(date_list, email_sender):
        for date_val in date_list:
            file_date = date_val.strftime('%Y-%m-%d')
            file_path = f"{hdfs_pd}/user/ZheS/wifi_score_v4/KPI/{file_date}"

            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)):
                print(f"data for {file_date} already exists.")
                continue

            try:
                analysis = wifiKPIAnalysis(date_val=date_val)
                analysis.run()
            except Exception as e:
                print(e)
                email_sender.send(
                                    send_from=f"wifiKPIAnalysis@verizon.com",
                                    subject=f"wifiKPIAnalysis failed !!! at {file_date}",
                                    text=str(e)
                                )

    process_kpi_data(date_list, email_sender)

