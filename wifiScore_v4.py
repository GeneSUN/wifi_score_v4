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
        
        titan_filter = col("model_name").isin( ["ASK-NCQ1338FA","ASK-NCQ1338","WNC-CR200A"] )

        df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .select("rowkey",explode("Group_Data_sys_info"))\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                            .select("sn",col("col.model").alias("model_name") )\
                            .filter(titan_filter)

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
            ip_change_file_path_template = hdfs_pd + "/user/ZheS/wifi_score_v4/time_window/ip_change_daily_df/{}"

        df_restart = df_owl.select("sn", "ts", "Diag_Result_dev_restart")\
                                .filter(F.col("Diag_Result_dev_restart").isNotNull())\
                                .groupby("sn")\
                                .agg(F.count("*").alias("no_reboot"))

        previous_day = self.date_val - timedelta(1)
        lookback_days = 4
        date_range_list = [(previous_day - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_ip_change_df = process_parquet_files_for_date_range(date_range_list, ip_change_file_path_template)
        historical_ip_change_df = historical_ip_change_df.groupby("sn", "model_name")\
                                                        .agg(F.sum("no_ip_changes").alias("no_ip_changes"))

        ip_change_cat_df = (
            historical_ip_change_df.join(df_restart, on="sn", how="left")\
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
        return ip_change_cat_df

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
                                            count(  when(  (col("action") == "1")&( F.col("sta_type") == "2" ) , True)).alias("band_success_count"),
                                            count(  when(  ( F.col("sta_type") == "2" ) , True)).alias("band_total_count"),
                                            #count("*").alias("total_")
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

        son_df = df_bandsteer.join(df_apsteer, on="sn", how="full_outer")\
                                    .withColumn(
                                        "success_count",
                                        F.coalesce(col("band_success_count"), lit(0)) + F.coalesce(col("ap_success_count"), lit(0))
                                    ).withColumn(
                                        "total_count",
                                        F.coalesce(col("band_total_count"), lit(0)) + F.coalesce(col("ap_total_count"), lit(0))
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
        restart_daily_file_path_template = hdfs_pd + "/user/ZheS/wifi_score_v4/time_window/restart_daily_df/{}"
        previous_day = self.date_val
        lookback_days = 4
        date_range_list = [(previous_day - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(lookback_days)]
        historical_restart_daily_df = process_parquet_files_for_date_range(date_range_list, restart_daily_file_path_template)
        historical_restart_daily_df.groupby("sn","model_name")\
                                    .agg( F.sum("num_mdm_crashes").alias("num_mdm_crashes"),
                                            F.sum("num_user_reboots").alias("num_user_reboots"),
                                            F.sum("num_total_reboots").alias("num_total_reboots"),
                                )\

        df_start = historical_restart_daily_df\
                        .withColumn("reboot_category", categorize_reboots_per_home("num_total_reboots"))\
                        .withColumn("modem_reset_category", categorize_modem_resets_per_home("num_mdm_crashes"))\
                        .withColumn("customer_reboot_category", categorize_customer_initiated_reboots("num_user_reboots"))
        
        return df_start

    def run(self):

        self.load_data()
        ip_change_cat_df = self.calculate_ip_changes()
        ip_change_cat_df.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/KPI/ip_change_cat_df/{(self.date_val).strftime('%Y-%m-%d')}")
        son_df = self.calculate_steer()
        son_df.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/KPI/son_df/{(self.date_val).strftime('%Y-%m-%d')}")
        df_restart = self.calculate_restart()
        df_restart.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/KPI/df_rbt/{(self.date_val).strftime('%Y-%m-%d')}")
        

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

