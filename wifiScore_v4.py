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

        lookback_days = 10
        date_range_list = [(self.date_val - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_ip_change_df = process_parquet_files_for_date_range(date_range_list, ip_change_file_path_template)
        historical_ip_change_df = historical_ip_change_df.groupby("sn")\
                                                        .agg(F.sum("no_ip_changes").alias("no_ip_changes"))

        ip_change_cat_df = (
            historical_ip_change_df.withColumn(
                                            "ip_change_category",
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
                                                        .agg(F.sum("no_poor_airtime").alias("no_poor_airtime"))

        airtime_df = (
            historical_airtime_df.withColumn(
                                                "Airtime_Utilization_Category",
                                                F.when(F.col("no_poor_airtime") > 200, "Poor") 
                                                 .when((F.col("no_poor_airtime") >= 75) & (F.col("no_poor_airtime") <= 200), "Fair") 
                                                 .when((F.col("no_poor_airtime") >= 26) & (F.col("no_poor_airtime") <= 74), "Good")  
                                                 .when(F.col("no_poor_airtime") >= 0, "Excellent") 
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

        df_distinct_rowkey_count = df_sh.groupBy("sn")\
                                        .agg( F.countDistinct("rowkey").alias("distinct_rowkey_count")  )

        son_df = df_bandsteer.join(df_apsteer, on="sn", how="full_outer")\
                            .withColumn(
                                "steer_start_count",
                                F.coalesce(col("band_success_count"), lit(0)) + F.coalesce(col("ap_success_count"), lit(0))
                            )\
                            .withColumn(
                                "steer_failure_count",
                                F.coalesce(col("band_failure_count"), lit(0)) + F.coalesce(col("ap_failure_count"), lit(0))
                            )\
                            .withColumn(
                                "total_count",
                                F.coalesce(col("band_total_count"), lit(0)) + F.coalesce(col("ap_total_count"), lit(0))
                            )\
                            .withColumn(
                                        "steer_start_category",
                                        F.when(F.col("steer_start_count") > 60, "Poor")
                                        .when((F.col("steer_start_count") >= 31) & (F.col("steer_start_count") <= 60), "Fair")
                                        .when((F.col("steer_start_count") >= 11) & (F.col("steer_start_count") <= 29), "Good")
                                        .when((F.col("steer_start_count").isNull() ) | (F.col("steer_start_count") <= 10), "Excellent")
                                        .otherwise("Unknown")
                                    )\
                            .join( df_distinct_rowkey_count, "sn" )\
                            .withColumn(
                                "steer_start_perHome_count",
                                F.col("steer_start_count")/F.col("distinct_rowkey_count")
                            )\
                            .withColumn(
                                        "steer_start_perHome_category",
                                        F.when(F.col("steer_start_perHome_count") > 3, "Poor")
                                        .when((F.col("steer_start_perHome_count") > 2) & (F.col("steer_start_perHome_count") <= 3), "Fair")
                                        .when((F.col("steer_start_perHome_count") > 1) & (F.col("steer_start_perHome_count") <= 2), "Good")
                                        .when((F.col("steer_start_perHome_count").isNull() ) | (F.col("steer_start_perHome_count") <= 1), "Excellent")
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
        lookback_days = 10
        date_range_list = [(previous_day - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_restart_daily_df = process_parquet_files_for_date_range(date_range_list, restart_daily_file_path_template)
        historical_restart_daily_df = historical_restart_daily_df.groupby("sn")\
                                                                    .agg( F.sum("num_mdm_crashes").alias("mdm_resets_count"),
                                                                            F.sum("num_user_reboots").alias("user_reboot_count"),
                                                                            F.sum("num_total_reboots").alias("total_reboots_count"),
                                                                )\

        df_start = historical_restart_daily_df\
                        .withColumn("reboot_category", categorize_reboots_per_home("total_reboots_count"))\
                        .withColumn("modem_reset_category", categorize_modem_resets_per_home("mdm_resets_count"))\
                        .withColumn("customer_reboot_category", categorize_customer_initiated_reboots("user_reboot_count"))
        
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
                            .withColumn("byte_send", F.col("Station_Data_connect_data.diff_bs"))\
                            .withColumn("byte_received", F.col("Station_Data_connect_data.diff_br"))\
                            .filter( col("signal_strength").isNotNull() )\
                            .withColumn("signal_strength_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("signal_strength")))\
                            .withColumn("signal_strength_5GHz",  F.when(F.col("connect_type") == "5G", F.col("signal_strength")))\
                            .withColumn("signal_strength_6GHz", F.when(F.col("connect_type") == "6G", F.col("signal_strength")) )\
                            .select("sn","rowkey","ts","connect_type","signal_strength", "signal_strength_2_4GHz","signal_strength_5GHz","signal_strength_6GHz",
                                    "byte_send","byte_received")
        
        thresholds = {
                "2_4GHz": {"Poor": -78, "Fair": -71, "Good": -56, },
                "5GHz": {"Poor": -75, "Fair": -71, "Good": -56, },
                "6GHz": {"Poor": -70, "Fair": -65, "Good": -56, }
            }


        def categorize_signal(df, signal_col, freq_band):

            return df.withColumn(f"category_{freq_band}", F.when(F.col(signal_col) < thresholds[freq_band]["Poor"], "Poor")
                                                            .when(F.col(signal_col).between(thresholds[freq_band]["Poor"], thresholds[freq_band]["Fair"]), "Fair")
                                                            .when(F.col(signal_col).between(thresholds[freq_band]["Fair"] - 1, thresholds[freq_band]["Good"]), "Good")
                                                            .when(F.col(signal_col) > thresholds[freq_band]["Good"], "Excellent")
                                                            .otherwise("No Data"))
                
        df_categorized = categorize_signal(df_flattened, "signal_strength_2_4GHz", "2_4GHz")
        df_categorized = categorize_signal(df_categorized, "signal_strength_5GHz", "5GHz")
        df_categorized = categorize_signal(df_categorized, "signal_strength_6GHz", "6GHz")

        df_grouped = df_categorized.groupBy("sn", "rowkey")\
                                    .agg(
                                            F.count("*").alias("total_count_rssi"),
                                            F.sum(F.when(F.col("category_2_4GHz") == "Poor", 1).otherwise(0)).alias("poor_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Poor", 1).otherwise(0)).alias("poor_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Poor", 1).otherwise(0)).alias("poor_count_6GHz"),
                                            
                                            F.sum(F.when(F.col("category_2_4GHz") == "Fair", 1).otherwise(0)).alias("fair_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Fair", 1).otherwise(0)).alias("fair_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Fair", 1).otherwise(0)).alias("fair_count_6GHz"),
                                        
                                            F.sum(F.when(F.col("category_2_4GHz") == "Good", 1).otherwise(0)).alias("good_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Good", 1).otherwise(0)).alias("good_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Good", 1).otherwise(0)).alias("good_count_6GHz"),
                                        
                                            F.sum(F.when(F.col("category_2_4GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_6GHz"),
                                            F.sum("byte_send").alias("byte_send"),
                                            F.sum("byte_received").alias("byte_received")
                                        )

        total_volume_window = Window.partitionBy("sn") 
        df_rowkey_rssi_category = df_grouped.withColumn("rssi_category_rowkey", 
                                                        F.when(
                                                                (F.col("poor_count_2_4GHz") >= 12) | (F.col("poor_count_5GHz") >= 12) | (F.col("poor_count_6GHz") >= 12), "Poor")
                                                                .when((F.col("fair_count_2_4GHz") >= 12) | (F.col("fair_count_5GHz") >= 12) | (F.col("fair_count_6GHz") >= 12), "Fair")
                                                                .when((F.col("good_count_2_4GHz") >= 12) | (F.col("good_count_5GHz") >= 12) | (F.col("good_count_6GHz") >= 12), "Good")
                                                                .when((F.col("excellent_count_2_4GHz") >= 12) | (F.col("excellent_count_5GHz") >= 12) | (F.col("excellent_count_6GHz") >= 12), "Excellent")
                                                                .otherwise("No Data")
                                                    )\
                                            .withColumn("volume",F.log( col("byte_send")+col("byte_received") ))\
                                            .withColumn("total_volume", F.sum("volume").over(total_volume_window))\
                                            .withColumn("weights", F.col("volume") / F.col("total_volume") )
    
        df_rowkey_rssi_numeric = df_rowkey_rssi_category.withColumn(
                                                                    "rssi_numeric_rowkey",
                                                                    F.when(F.col("rssi_category_rowkey") == "Poor", 1)
                                                                    .when(F.col("rssi_category_rowkey") == "Fair", 2)
                                                                    .when(F.col("rssi_category_rowkey") == "Good", 3)
                                                                    .when(F.col("rssi_category_rowkey") == "Excellent", 4)
                                                                )\
                                                        .groupBy("sn")\
                                                        .agg(  
                                                            F.round(F.sum(col("rssi_numeric_rowkey") * col("weights")), 4).alias("rssi_numeric"),  
                                                        )
        
        df_final = df_rowkey_rssi_numeric.withColumn( "RSSI_category", 
                                                    F.when(F.col("rssi_numeric") <= 1.5, "Poor")\
                                                    .when((F.col("rssi_numeric") > 1.5) & (F.col("rssi_numeric") <= 2.5), "Fair")\
                                                    .when((F.col("rssi_numeric") > 2.5) & (F.col("rssi_numeric") <= 3.5), "Good")\
                                                    .when(F.col("rssi_numeric") > 3.5, "Excellent")  )

        return df_final
    
    def calculate_phyrate(self, df_sh = None):
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
                            .filter( col("connect_type").isin( ["2_4G","5G","6G"]) )\
                            .withColumn("byte_send", F.col("Station_Data_connect_data.diff_bs"))\
                            .withColumn("byte_received", F.col("Station_Data_connect_data.diff_br"))\
                            .withColumn("tx_link_rate", F.col("Station_Data_connect_data.tx_link_rate"))\
                            .withColumn("tx_link_rate", F.regexp_replace(F.col("tx_link_rate"), "Mbps", "") )\
                            .withColumn("tx_link_rate_2_4GHz", F.when(F.col("connect_type") == "2_4G", F.col("tx_link_rate")))\
                            .withColumn("tx_link_rate_5GHz",  F.when(F.col("connect_type") == "5G", F.col("tx_link_rate")))\
                            .withColumn("tx_link_rate_6GHz", F.when(F.col("connect_type") == "6G", F.col("tx_link_rate")) )\
                            .select("sn","rowkey","ts","connect_type","tx_link_rate", "tx_link_rate_2_4GHz","tx_link_rate_5GHz","tx_link_rate_6GHz",
                                     "byte_send","byte_received")

        thresholds = {
                "2_4GHz": {"Poor": 80, "Fair": 100, "Good": 120, },
                "5GHz": {"Poor": 200, "Fair": 300, "Good": 500, },
                "6GHz": {"Poor": 200, "Fair": 300, "Good": 500, }
            }

        def categorize_signal(df, phyrate_col, freq_band):

            return df.withColumn(f"category_{freq_band}", F.when(F.col(phyrate_col) < thresholds[freq_band]["Poor"], "Poor")
                                                            .when(F.col(phyrate_col).between(thresholds[freq_band]["Poor"], thresholds[freq_band]["Fair"]), "Fair")
                                                            .when(F.col(phyrate_col).between(thresholds[freq_band]["Fair"] + 1, thresholds[freq_band]["Good"]), "Good")
                                                            .when(F.col(phyrate_col) > thresholds[freq_band]["Good"], "Excellent")
                                                            .otherwise("No Data"))
                
        df_categorized = categorize_signal(df_flattened, "tx_link_rate_2_4GHz", "2_4GHz")
        df_categorized = categorize_signal(df_categorized, "tx_link_rate_5GHz", "5GHz")
        df_categorized = categorize_signal(df_categorized, "tx_link_rate_6GHz", "6GHz")

        df_grouped = df_categorized.groupBy("sn", "rowkey")\
                                    .agg(
                                            F.sum("byte_send").alias("byte_send"),
                                            F.sum("byte_received").alias("byte_received"),
                                            F.count("*").alias("total_count_phyrate"),
                                            F.sum(F.when(F.col("category_2_4GHz") == "Poor", 1).otherwise(0)).alias("poor_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Poor", 1).otherwise(0)).alias("poor_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Poor", 1).otherwise(0)).alias("poor_count_6GHz"),
                                            
                                            F.sum(F.when(F.col("category_2_4GHz") == "Fair", 1).otherwise(0)).alias("fair_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Fair", 1).otherwise(0)).alias("fair_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Fair", 1).otherwise(0)).alias("fair_count_6GHz"),
                                        
                                            F.sum(F.when(F.col("category_2_4GHz") == "Good", 1).otherwise(0)).alias("good_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Good", 1).otherwise(0)).alias("good_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Good", 1).otherwise(0)).alias("good_count_6GHz"),
                                        
                                            F.sum(F.when(F.col("category_2_4GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_2_4GHz"),
                                            F.sum(F.when(F.col("category_5GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_5GHz"),
                                            F.sum(F.when(F.col("category_6GHz") == "Excellent", 1).otherwise(0)).alias("excellent_count_6GHz")
                                        )

        total_volume_window = Window.partitionBy("sn") 
        df_rowkey_phyrate_category = df_grouped.withColumn("rowkey_phyrate_category", 
                                                            F.when(
                                                                    (F.col("poor_count_2_4GHz") >= 12) | (F.col("poor_count_5GHz") >= 12) | (F.col("poor_count_6GHz") >= 12), "Poor")
                                                                    .when((F.col("fair_count_2_4GHz") >= 12) | (F.col("fair_count_5GHz") >= 12) | (F.col("fair_count_6GHz") >= 12), "Fair")
                                                                    .when((F.col("good_count_2_4GHz") >= 12) | (F.col("good_count_5GHz") >= 12) | (F.col("good_count_6GHz") >= 12), "Good")
                                                                    .when((F.col("excellent_count_2_4GHz") >= 12) | (F.col("excellent_count_5GHz") >= 12) | (F.col("excellent_count_6GHz") >= 12), "Excellent")
                                                                    .otherwise("No Data")
                                                        )\
                                                .withColumn("volume",F.log( col("byte_send")+col("byte_received") ))\
                                                .withColumn("total_volume", F.sum("volume").over(total_volume_window))\
                                                .withColumn("weights", F.col("volume") / F.col("total_volume") )
        
        df_rowkey_phyrate_numeric = df_rowkey_phyrate_category.withColumn(
                                                                        "phyrate_numeric",
                                                                        F.when(F.col("rowkey_phyrate_category") == "Poor", 1)
                                                                        .when(F.col("rowkey_phyrate_category") == "Fair", 2)
                                                                        .when(F.col("rowkey_phyrate_category") == "Good", 3)
                                                                        .when(F.col("rowkey_phyrate_category") == "Excellent", 4)
                                                                    )\
                                                        .groupBy("sn")\
                                                        .agg(  
                                                            F.round(F.sum(col("phyrate_numeric") * col("weights")), 4).alias("phyrate_numeric"),  
                                                        )
                                                                    
        df_final = df_rowkey_phyrate_numeric.withColumn( "phyrate_category", 
                                                    F.when(F.col("phyrate_numeric") <= 1.5, "Poor")\
                                                    .when((F.col("phyrate_numeric") > 1.5) & (F.col("phyrate_numeric") <= 2.5), "Fair")\
                                                    .when((F.col("phyrate_numeric") > 2.5) & (F.col("phyrate_numeric") <= 3.5), "Good")\
                                                    .when(F.col("phyrate_numeric") > 3.5, "Excellent")  )
        return df_final

    def calculate_sudden_drop(self, df_sh = None, date_val = None ):
        
        if df_sh is None:
            df_sh = self.df_sh
        if date_val is None:
            date_val = self.date_val
                
        df = df_sh.withColumn("signal_strength", F.col("Station_Data_connect_data.signal_strength"))\
                    .filter( col("signal_strength").isNotNull() )
                
        partition_columns = ["sn","rowkey"]
        column_name = "signal_strength"
        percentiles = [0.03, 0.1, 0.5, 0.9]
        window_spec = Window().partitionBy(partition_columns) 

        three_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[0]})') 
        ten_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[1]})') 
        med_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[2]})') 
        ninety_percentile = F.expr(f'percentile_approx({column_name}, {percentiles[3]})') 

        df_outlier = df.withColumn('3%_val', three_percentile.over(window_spec))\
                        .withColumn('10%_val', ten_percentile.over(window_spec))\
                        .withColumn('50%_val', med_percentile.over(window_spec))\
                        .withColumn('90%_val', ninety_percentile.over(window_spec))\
                        .withColumn("lower_bound", col('10%_val')-2*(  col('90%_val') - col('10%_val') ) )\
                        .withColumn("outlier", when( col("lower_bound") < col("3%_val"), col("lower_bound")).otherwise( col("3%_val") ))\
                        .filter(col(column_name) > col("outlier"))

        stationary_daily_df = df_outlier.withColumn("diff", col('90%_val') - col('50%_val') )\
                                .withColumn("stationarity", when( col("diff")<= 3, lit("1")).otherwise( lit("0") ))\
                                .groupby(partition_columns).agg(max("stationarity").alias("stationarity"))\
                                .filter( col("stationarity")>0 )

        df_ts_station = df_sh.join( stationary_daily_df, ["sn","rowkey"])\
                                .filter( col("rowkey").isNotNull() )\
                                .groupBy("sn","datetime")\
                                .agg(  F.count("rowkey").alias("station_cnt") )

        window_spec = Window.partitionBy("sn").orderBy("datetime")
        df_lagged = df_ts_station.withColumn( "prev_station_cnt",  F.lag("station_cnt").over(window_spec) )\
                                        .withColumn( "drop_diff", F.col("prev_station_cnt") - F.col("station_cnt") )\
                                        .filter( F.col("drop_diff") > 3 )

        df_lagged_filtered = df_lagged.withColumn(
                                                    "drop_diff", F.col("prev_station_cnt") - F.col("station_cnt")
                                        ).filter(
                                                F.col("drop_diff") > 3
                                            )

        df_result = df_lagged_filtered.groupBy("sn")\
                                        .agg( F.count("drop_diff").alias("no_sudden_drop") )\
                                        .withColumn( "sudden_drop_category", 
                                                    F.when(F.col("no_sudden_drop") >= 2, "Poor")\
                                                    .when( F.col("no_sudden_drop") == 1, "Fair")\
                                                    .when( F.col("no_sudden_drop").isNull(), "Excellent")\
                                                    .otherwise("Good")
                                                )

        return df_result

    def run(self):

        self.load_data()

        ip_change_cat_df = self.calculate_ip_changes()
        son_df = self.calculate_steer()
        df_restart = self.calculate_restart()
        df_rssi = self.calculate_rssi()
        df_phyrate = self.calculate_phyrate()
        df_airtime = self.calculate_airtime()
        df_sudden_drop = self.calculate_sudden_drop()

        df_full_joined = ip_change_cat_df.join(son_df, on="sn", how="full_outer")\
                                        .join(df_restart, on="sn", how="full_outer")\
                                        .join(df_rssi, on="sn", how="full_outer")\
                                        .join(df_phyrate, on="sn", how="full_outer")\
                                        .join(df_airtime, on="sn", how="full_outer")\
                                        .join(df_sudden_drop, on="sn", how="full_outer")\
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

