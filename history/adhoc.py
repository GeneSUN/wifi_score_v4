from pyspark.sql import SparkSession
from pyspark.sql.types import (DateType, DoubleType, StringType, StructType, StructField) 
from pyspark.sql import functions as F
from pyspark.sql.functions import ( 
    abs, avg, broadcast, col,concat, concat_ws, countDistinct, desc, exp, expr, explode, first, from_unixtime, 
    lpad, length, lit, max, min, rand, regexp_replace, round,struct, sum, to_json,to_date, udf, when, 
) 
import json

import argparse
from datetime import datetime, timedelta, date
import time 

import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS/OOP_dir') 
from MailSender import MailSender

def flatten_df_v2(nested_df):
    # flat the nested columns and return as a new column
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']
    array_cols = [c[0] for c in nested_df.dtypes if c[1][:5] == "array"]
    #print(len(nested_cols))
    if len(nested_cols)==0 and len(array_cols)==0 :
        #print(" dataframe flattening complete !!")
        return nested_df
    elif len(nested_cols)!=0:
        flat_df = nested_df.select(flat_cols +
                                   [F.col(nc+'.'+c).alias(nc+'_b_'+c)
                                    for nc in nested_cols
                                    for c in nested_df.select(nc+'.*').columns])
        return flatten_df_v2(flat_df)
    elif len(array_cols)!=0:
        for array_col in array_cols:
            flat_df = nested_df.withColumn(array_col, F.explode(F.col(array_col)))
        return flatten_df_v2(flat_df) 

from pyspark.sql import functions as F
from pyspark.sql import Window
from datetime import date

class wifiProcessor:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self, date_val):
        date_compact = date_val.strftime("%Y%m%d") # e.g. 20231223
        date_iso     = date_val.strftime("%Y-%m-%d") # e.g. 2023-12-23
        self.station_history_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{date_compact}"
        self.df_sh = spark.read.parquet(self.station_history_path).transform(flatten_df_v2)
        self.df_owl = spark.read.parquet(f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{date_compact}")
        self.df_device = spark.read.parquet( hdfs_pd + f"/usr/apps/vmas/sha_data/bhrx_hourly_data/Devices/{date_compact}" )

    def get_top_10_station_names(self, df_sh = None, rowkey_column="rowkey", station_name_column="station_name"):
        if df_sh is None:
            df_sh = self.df_sh

        count_df = df_sh.withColumn("station_name", F.col("Station_Data_connect_data_b_station_name") )\
                        .groupBy(rowkey_column, station_name_column) \
                        .agg(F.count("*").alias("count"))

        window_spec = Window.partitionBy(rowkey_column).orderBy(F.desc("count"))
        ranked_df = count_df.withColumn("rank", F.row_number().over(window_spec))
        top_10_df = ranked_df.filter(F.col("rank") <= 10)

        result_df = top_10_df.groupBy(rowkey_column) \
                             .agg(F.collect_list(station_name_column).alias("top_10_station_names"))

        return result_df

    def get_rssi_phyrate(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh        

        # as long as sdcd_connect_type is 2.4G/5G/6G, sdcd_signal_strength/sdcd_tx_link_rate cannot be null
        df_signal = df_sh.withColumn('sdcd_connect_type', F.col("Station_Data_connect_data_b_connect_type").substr(F.lit(1), F.lit(2)))\
                    .filter( (col("sdcd_connect_type").isin(['2.','5G','6G'])) )\
                    .withColumn("sdcd_signal_strength", F.col("Station_Data_connect_data_b_signal_strength").cast("long"))\
                    .withColumn('sdcd_tlr_length', F.length('Station_Data_connect_data_b_tx_link_rate'))\
                    .withColumn('sdcd_tx_link_rate', F.col('Station_Data_connect_data_b_tx_link_rate').substr(F.lit(1), F.col('sdcd_tlr_length') - F.lit(4)))\
                    .withColumn("sdcd_tx_link_rate",col("sdcd_tx_link_rate").cast("long"))\
                    .groupBy("rowkey")\
                    .agg(  F.round( F.avg(F.when(F.col("sdcd_connect_type") == "2.", F.col("sdcd_signal_strength"))), 2).alias("avg_2_4_signal_strength"),
                           F.round(  F.avg(F.when(F.col("sdcd_connect_type") == "5G", F.col("sdcd_signal_strength"))), 2).alias("avg_5G_signal_strength"), 
                           F.round(  F.avg(F.when(F.col("sdcd_connect_type") == "6G", F.col("sdcd_signal_strength"))), 2).alias("avg_6G_signal_strength")
                        )
        return df_signal

    def get_dataconsumption(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh

        df_dataconsumption = df_sh.withColumn("byte_send", F.col("Station_Data_connect_data_b_diff_bs").cast("long"))\
                                    .withColumn("byte_received", F.col("Station_Data_connect_data_b_diff_br").cast("long"))\
                                    .groupby("rowkey")\
                                    .agg(
                                        F.sum("byte_send").alias("total_byte_send"),
                                        F.sum("byte_received").alias("total_byte_received")
                                    )
        
        return df_dataconsumption

    def get_mode(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh

        df_mode = df_sh.withColumn("mode", F.col("Station_Data_connect_data_b_mode").cast("numeric"))\
                        .filter(col("mode")<=11 )\
                        .filter(col("mode")>=5 )\
                        .groupby("rowkey")\
                        .agg( F.count("*").alias("total_count_mode"))
        
        return df_mode

    def get_bandwidth(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
        df_bandwidth = df_sh.withColumn("bandwidth", F.col("Station_Data_connect_data_b_bw").cast("numeric"))\
                            .select("rowkey","ts","bandwidth" )\
                            .filter(col("bandwidth")>=0 )\
                            .filter(col("bandwidth")<=5 )\
                            .groupby("rowkey")\
                            .agg( F.avg("bandwidth").alias("avg_bandwidth") )

        return df_bandwidth

    def get_airtime_util(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
        df_airtime_util = df_sh.withColumn("airtime_util", F.col("Station_Data_connect_data_b_airtime_util").cast("numeric"))\
                                .select("rowkey","ts","airtime_util")\
                                .groupby("rowkey")\
                                .agg( F.avg("airtime_util").alias("avg_airtime_util") )
        
        return df_airtime_util

    def get_channel_operation(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh
        df_channel_operation = df_sh.withColumn("channel", F.col("Station_Data_connect_data_b_channel").cast("numeric"))\
                                .select("rowkey","ts","channel")\
                                .groupby("rowkey")\
                                .agg( F.avg("channel").alias("avg_channel") )

        return df_channel_operation
    
    def get_band_steer(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh

        df_band_steer = df_sh.filter(col("Diag_Result_band_steer_b_sta_type")=="2" )\
                            .groupby("rowkey")\
                            .agg( F.count("*").alias("band_steer"))

        return df_band_steer
    
    def get_ap_steer(self, df_sh = None):
        if df_sh is None:
            df_sh = self.df_sh

        df_ap_steer = df_sh.filter(col("Diag_Result_ap_steer_b_sta_type")=="2" )\
                            .groupby("rowkey")\
                            .agg( F.count("*").alias("ap_steer"))

        return df_ap_steer

    def get_restart(self, df_owl = None):
        if df_owl is None:
            df_owl = self.df_owl
        df_restart = df_owl.select("rowkey","ts","Diag_Result_dev_restart")\
                            .filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason")) \
                            .filter(col("reason")=="GUI")\
                            .groupby("rowkey")\
                            .agg( F.count("*").alias("restart_GUI"))
        return df_restart

    def get_Frequent_channel_switch(self, df_owl = None):
        if df_owl is None:
            df_owl = self.df_owl
        df_ch_change = df_owl.select("rowkey","ts","owl_data_acs_ch_change")\
                            .withColumn("reason", F.col("owl_data_acs_ch_change.reason")) \
                            .groupby("rowkey")\
                            .agg( F.count("*").alias("ch_change"))
        return df_ch_change
    
    def get_neighbors(self, df_device = None):
        if df_device is None:
            df_device = self.df_device
        df_6g_group = df_device.withColumn("wifi_ap_data_6g_exploded", F.explode("wifi_scan_wifi_ap_data_6g"))\
                                .withColumn("wifi_6g_data_exploded", F.explode("wifi_ap_data_6g_exploded.data"))\
                                .groupBy("rowkey").agg(F.countDistinct("wifi_6g_data_exploded.bssid").alias("distinct_6g_bssid_count"))
        
        df_24g_group= df_device.withColumn("wifi_ap_data_2_4g_exploded", F.explode("wifi_scan_wifi_ap_data_2_4g"))\
                                .withColumn("wifi_data_2_4g_exploded", F.explode("wifi_ap_data_2_4g_exploded.data"))\
                                .groupBy("rowkey").agg(F.countDistinct("wifi_data_2_4g_exploded.bssid").alias("distinct_2_4g_bssid_count"))

        df_5g_group = df_device.withColumn("wifi_ap_data_5g_exploded", F.explode("wifi_scan_wifi_ap_data_5g"))\
                                .withColumn("wifi_5g_data_exploded", F.explode("wifi_ap_data_5g_exploded.data"))\
                                .groupBy("rowkey").agg(F.countDistinct("wifi_5g_data_exploded.bssid").alias("distinct_5g_bssid_count"))

        df_neighbor = df_24g_group.join( df_5g_group, on = "rowkey", how = "outer" )\
                                .join( df_6g_group, on = "rowkey", how = "outer" )
        
        return df_neighbor


    def join_all(self):
        # Get all the DataFrames by calling their respective methods
        df_top_10_station_names = self.get_top_10_station_names()
        df_rssi_phyrate = self.get_rssi_phyrate()
        df_dataconsumption = self.get_dataconsumption()
        df_mode = self.get_mode()
        df_bandwidth = self.get_bandwidth()
        df_airtime_util = self.get_airtime_util()
        df_channel_operation = self.get_channel_operation()
        df_band_steer = self.get_band_steer()
        df_ap_steer = self.get_ap_steer()
        df_restart = self.get_restart()
        df_Frequent_channel_switch = self.get_Frequent_channel_switch()
        df_neighbors = self.get_neighbors()

        df_result = df_top_10_station_names.join(df_rssi_phyrate, on="rowkey", how="outer")\
                                           .join(df_dataconsumption, on="rowkey", how="outer")\
                                           .join(df_mode, on="rowkey", how="outer")\
                                           .join(df_bandwidth, on="rowkey", how="outer")\
                                           .join(df_airtime_util, on="rowkey", how="outer")\
                                           .join(df_channel_operation, on="rowkey", how="outer")\
                                           .join(df_band_steer, on="rowkey", how="outer")\
                                           .join(df_ap_steer, on="rowkey", how="outer")\
                                           .join(df_restart, on="rowkey", how="outer")\
                                           .join(df_Frequent_channel_switch, on="rowkey", how="outer")\
                                           #.join(df_neighbors, on="rowkey", how="outer")

        return df_result


if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('wifiscore_v4_adhoc')\
                        .master("spark://njbbvmaspd11.nss.vzwnet.com:7077")\
                        .config("spark.ui.port","24045")\
                        .enableHiveSupport().getOrCreate()
    mail_sender = MailSender() 
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    day_before = 1
    date_val = ( date.today() - timedelta(day_before) )

    ins = wifiProcessor( date_val )
    ins.join_all()\
        .repartition(10)\
                    .withColumn("date", F.lit((date_val - timedelta(1)).strftime('%Y-%m-%d')))\
                    .write\
                    .parquet( hdfs_pd + "/user/ZheS/wifi_score_v4/deviceScore_dataframe/"+ (date_val- timedelta(1)).strftime("%Y-%m-%d") )

