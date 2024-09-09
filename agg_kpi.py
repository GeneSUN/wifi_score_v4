
from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list, concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 
from dateutil.parser import parse
import argparse 
from functools import reduce 
import sys
import numpy as np

class wifiKPIAggregator:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self, date_val):
        self.date_val = date_val
        self.owl_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{ date_val.strftime('%Y%m%d')  }"
        self.station_history_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{ date_val.strftime('%Y%m%d')  }"
        self.deviceGroup_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/{ date_val.strftime('%Y%m%d')  }"

        self.load_data()

    def load_data(self):
        
        titan_filter = col("model_name").isin( ["ASK-NCQ1338FA","ASK-NCQ1338","WNC-CR200A"] )

        df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .select("rowkey",explode("Group_Data_sys_info"))\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                            .select("sn",col("col.model").alias("model_name") )\
                            .distinct()\
                            .filter(titan_filter)

        self.df_owl = spark.read.parquet( self.owl_path )\
            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
            .join( df_dg, "sn" )
        
        self.df_sh = spark.read.parquet(self.station_history_path)\
                    .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                    .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
                    .join( df_dg, "sn" )

    def ip_changes_agg(self, df_owl = None, df_restart = None):
        
        if df_owl is None:
            df_owl = self.df_owl

        window_spec = Window.partitionBy("model_name", "sn").orderBy("ts")

        ip_change_daily_df = (
            df_owl.withColumn("day", F.to_date("datetime") )\
                    .filter(F.col("owl_data_fwa_cpe_data").isNotNull())\
                    .withColumn("ipv4_ip", F.get_json_object(F.col("Owl_Data_fwa_cpe_data"), "$.ipv4_ip"))\
                    .filter(F.col("ipv4_ip").isNotNull())\
                    .withColumn("prev_ip4", F.lag("ipv4_ip").over(window_spec))\
                    .withColumn("ip_changes_flag",
                                F.when((F.col("ipv4_ip") != F.col("prev_ip4")) & F.col("prev_ip4").isNotNull(), 1).otherwise(0))\
                    .groupby("sn", "model_name")\
                    .agg(F.sum("ip_changes_flag").alias("no_ip_changes"))
            )
        return ip_change_daily_df

    def restart_agg(self, df_owl = None):
        
        if df_owl is None:
            df_owl = self.df_owl

        df_modem_crash = df_owl.filter( (F.col("owl_data_modem_event").isNotNull()) )\
                                .groupBy("sn") \
                                .agg(F.count("*").alias("num_mdm_crashes"))

        df_user_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .filter(col("reason").isin(["GUI","APP","BTN"]) )\
                            .groupby("sn")\
                            .agg( F.count("*").alias("num_user_reboots"))

        df_total_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .groupby("sn")\
                            .agg( F.count("*").alias("num_total_reboots"))
        
        df_restart = df_modem_crash.join(df_user_rbt, on="sn", how="full_outer")\
                                .join(df_total_rbt, on="sn", how="full_outer")\
        
        return df_restart

    def run(self):

        self.load_data()
        ip_change_daily_df = self.ip_changes_agg()
        ip_change_daily_df.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/ip_change_daily_df/{(self.date_val - timedelta(1)).strftime('%Y-%m-%d')}")
        restart_daily_df = self.restart_agg()
        restart_daily_df.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/restart_daily_df/{(self.date_val - timedelta(1)).strftime('%Y-%m-%d')}")
        
if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_agg')\
                        .config("spark.sql.adapative.enabled","true")\
                        .config("spark.ui.port","24043")\
                        .getOrCreate()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'

    parser = argparse.ArgumentParser(description="Inputs")
    day_before = 1
    date_val = ( date.today() - timedelta(day_before) )

    analysis = wifiKPIAggregator(  date_val = date_val)
    analysis.run()








