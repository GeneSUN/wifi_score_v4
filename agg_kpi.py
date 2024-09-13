
from pyspark.sql import functions as F 
from pyspark.sql.functions import (collect_list, concat,from_unixtime,lpad, broadcast, sum, udf, col, abs, length, min, max, lit, avg, when, concat_ws, to_date, exp, explode,countDistinct, first,round  ) 
from pyspark.sql import SparkSession 
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta 
from py4j.java_gateway import java_import
from dateutil.parser import parse
import argparse 
from functools import reduce 
import sys
import numpy as np
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
class wifiKPIAggregator:
    global hdfs_pd, hdfs_pa
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    def __init__(self, date_val):
        self.date_val = date_val
        self.owl_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{ (date_val + timedelta(1)).strftime('%Y%m%d')  }"
        self.station_history_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{ (date_val + timedelta(1)).strftime('%Y%m%d')  }"
        self.deviceGroup_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/{ (date_val + timedelta(1)).strftime('%Y%m%d')  }"
        self.load_data()
        self.ip_change_daily_df = self.ip_changes_agg()
        self.restart_daily_df = self.restart_agg()
        self.airtime_daily_df = self.airtime_agg()
        self.stationary_daily_df = self.stationary_agg()

    def load_data(self):
        
        self.df_owl = spark.read.parquet( self.owl_path )\
            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
            .withColumn("day", F.to_date("datetime") )
        
        self.df_sh = spark.read.parquet(self.station_history_path)\
                    .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                    .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
                    .withColumn("day", F.to_date("datetime") )

        self.df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
                            .withColumn("day", F.to_date("datetime") )


    def airtime_agg(self, df_dg = None):
        if df_dg is None:
            df_dg = self.df_dg

        airtime_util_history = df_dg.filter( F.col("Group_Diag_History_radio_wifi_info").isNotNull() )\
                                    .select(
                                            F.col("sn"),
                                            F.col("Group_Diag_History_radio_wifi_info.ts").alias("ts"),
                                            F.col("Group_Diag_History_radio_wifi_info._2_4g.enable").alias("enable"),
                                            F.col("Group_Diag_History_radio_wifi_info._2_4g.airtime_util").alias("airtime_util")
                                        )\
                                    .withColumn(
                                            "zipped", 
                                            F.arrays_zip( F.col("ts"),F.col("airtime_util"), F.col("enable"))
                                        )\
                                    .withColumn("zipped_explode", F.explode("zipped"))\
                                    .select(
                                            "sn",
                                            F.col("zipped_explode.ts").alias("ts"),
                                            F.col("zipped_explode.airtime_util").alias("airtime_util"),
                                            F.col("zipped_explode.enable").alias("enable")
                                        )\
                                    .filter( col("enable")==1 )

        df_airtime = airtime_util_history.groupby("sn")\
                                        .agg( F.sum("airtime_util").alias("sum_airtime_util") )

        return df_airtime

    def ip_changes_agg(self, df_owl = None ):
        
        if df_owl is None:
            df_owl = self.df_owl

        window_spec = Window.partitionBy("sn").orderBy("ts")

        ip_change_daily_df = (
            df_owl.filter(F.col("owl_data_fwa_cpe_data").isNotNull())\
                    .withColumn("ipv4_ip", F.get_json_object(F.col("Owl_Data_fwa_cpe_data"), "$.ipv4_ip"))\
                    .filter(F.col("ipv4_ip").isNotNull())\
                    .withColumn("prev_ip4", F.lag("ipv4_ip").over(window_spec))\
                    .withColumn("ip_changes_flag",
                                F.when((F.col("ipv4_ip") != F.col("prev_ip4")) & F.col("prev_ip4").isNotNull(), 1).otherwise(0))\
                    .groupby("sn","day")\
                    .agg(F.sum("ip_changes_flag").alias("no_ip_changes"))
            )
        return ip_change_daily_df

    def restart_agg(self, df_owl = None):
        
        if df_owl is None:
            df_owl = self.df_owl

        df_modem_crash = df_owl.filter( (F.col("owl_data_modem_event").isNotNull()) )\
                                .groupBy("sn","day") \
                                .agg(F.count("*").alias("num_mdm_crashes"))

        df_user_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .filter(col("reason").isin(["GUI","APP","BTN"]) )\
                            .groupby("sn","day")\
                            .agg( F.count("*").alias("num_user_reboots"))

        df_total_rbt = df_owl.filter(col("Diag_Result_dev_restart").isNotNull())\
                            .withColumn("reason", F.col("Diag_Result_dev_restart.reason"))\
                            .groupby("sn","day")\
                            .agg( F.count("*").alias("num_total_reboots"))
        
        df_restart = df_modem_crash.join(df_user_rbt, on=["sn","day"], how="full_outer")\
                                .join(df_total_rbt, on=["sn","day"], how="full_outer")\
        
        return df_restart

    def stationary_agg(self, df_sh = None, date_val = None ):
        
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
                                .withColumn("day", F.lit(F.date_format(F.lit(date_val), "MM-dd-yyyy")))

        return stationary_daily_df

if __name__ == "__main__":
    # the only input is the date which is used to generate 'date_range'
    spark = SparkSession.builder.appName('ZheS_wifiscore_agg')\
                        .config("spark.sql.adapative.enabled","true")\
                        .config("spark.ui.port","24043")\
                        .getOrCreate()
    
    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    email_sender = MailSender()
    hdfs_pd = 'hdfs://njbbvmaspd11.nss.vzwnet.com:9000/'
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    backfill_range = 4
    parser = argparse.ArgumentParser(description="Inputs") 
    parser.add_argument("--date", default=( date.today() - timedelta(1) ).strftime("%Y-%m-%d")) 
    args_date = parser.parse_args().date
    date_list = [( datetime.strptime( args_date, "%Y-%m-%d" )  - timedelta(days=i)).date() for i in range(backfill_range)][::-1]

    def process_kpi_data(date_list, file_type, email_sender):
        for date_val in date_list:
            file_date = date_val.strftime('%Y-%m-%d')
            file_path = f"{hdfs_pd}/user/ZheS/wifi_score_v4/time_window/{file_date}/{file_type}"

            if hadoop_fs.exists(spark._jvm.org.apache.hadoop.fs.Path(file_path)):
                print(f"{file_type} data for {file_date} already exists.")
                continue

            try:
                analysis = wifiKPIAggregator(date_val=date_val)
                getattr(  analysis, file_type  ).write.mode("overwrite").parquet(file_path)
            except Exception as e:
                print(e)
                email_sender.send(
                    send_from=f"{file_type}@verizon.com",
                    subject=f"{file_type} failed !!! at {file_date}",
                    text=str(e)
                )

    process_kpi_data(date_list, "ip_change_daily_df", email_sender)
    process_kpi_data(date_list, "restart_daily_df", email_sender)
    process_kpi_data(date_list, "airtime_daily_df", email_sender)
    process_kpi_data(date_list, "stationary_daily_df", email_sender)

