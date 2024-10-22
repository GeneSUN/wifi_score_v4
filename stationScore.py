from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import numpy as np
import sys 
import traceback
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
def read_file_by_date(date, file_path_pattern): 
    try:
        file_path = file_path_pattern.format(date) 
        df = spark.read.parquet(file_path) 
        return df 
    except:
        return None
    
class ScoreCalculator: 
    def __init__(self, weights): 
        self.weights = weights 
 
    def calculate_score(self, *args): 
        total_weight = 0 
        score = 0 

        for weight, value in zip(self.weights.values(), args): 
            if value is not None: 
                score += weight * float(value) 
                total_weight += weight 

        return score / total_weight if total_weight != 0 else None 
    
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
        self.model_sn_df = spark.read.parquet( self.deviceGroup_path )\
                                .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                                .select("sn",explode("Group_Data_sys_info"))\
                                .select("sn",F.col("col.model").alias("model_name") )\
                                .distinct()
        
        self.df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\

        self.df_owl = spark.read.parquet( self.owl_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
        
        self.df_sh = spark.read.parquet(self.station_history_path)\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))
    


    def calculate_rssi_sta(self, df_sh = None):
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
                                                                )

        return df_rowkey_rssi_numeric
    
    def calculate_phyrate_sta(self, df_sh = None):
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
    
            return df.withColumn(f"category_{freq_band}", 
                                    F.when(F.col(phyrate_col) < thresholds[freq_band]["Poor"], "Poor")
                                    .when((F.col(phyrate_col) >= thresholds[freq_band]["Poor"]) & (F.col(phyrate_col) < thresholds[freq_band]["Fair"]), "Fair")
                                    .when((F.col(phyrate_col) >= thresholds[freq_band]["Fair"]) & (F.col(phyrate_col) < thresholds[freq_band]["Good"]), "Good")
                                    .when(F.col(phyrate_col) >= thresholds[freq_band]["Good"], "Excellent")
                                    .otherwise("No Data")
                                    )


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
                                                                    )
        return df_rowkey_phyrate_numeric


if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    email_sender = MailSender()
    date_val = date.today() - timedelta(1)
    analysis = wifiKPIAnalysis(date_val=date_val)

    df_rssi = analysis.calculate_rssi_sta()
    df_phyrate = analysis.calculate_phyrate_sta()

    df_rssi.drop("byte_send","byte_received","volume","total_volume","weights","rssi_numeric_rowkey")\
            .join( df_phyrate.drop("byte_send","byte_received","volume","total_volume","rssi_numeric_rowkey"), 
                  ["sn","rowkey"] )\
            .write.mode("overwrite").parquet(hdfs_pd + f"/user/ZheS/wifi_score_v4/staScore" ) 