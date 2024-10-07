from pyspark.sql import SparkSession
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

class wifiKPIAnalysis:
    global hdfs_pd, hdfs_pa, cache_file
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    cache_file = "/user/ZheS/wifi_score_v4/cache"
    def __init__(self, date_val):
        self.date_val = date_val
        self.owl_path = f"{hdfs_pd}/{cache_file}/OWLHistory/{ (date_val+timedelta(1)).strftime('%Y-%m-%d')  }"
        self.station_history_path = f"{hdfs_pd}/{cache_file}/StationHistory/{ (date_val+timedelta(1)).strftime('%Y-%m-%d')  }"
        self.deviceGroup_path = f"{hdfs_pd}/{cache_file}/DeviceGroups/{ (date_val+timedelta(1)).strftime('%Y-%m-%d')  }"

    def load_data(self):
        exclude_time_condition = ((F.hour(F.col("datetime")) >= (11+4) ) & (F.hour(F.col("datetime")) < (14+4)  ))
        exclude_time_condition = ~exclude_time_condition
        """
        self.model_sn_df = spark.read.parquet( self.deviceGroup_path )\
                                .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
                                .select("sn",explode("Group_Data_sys_info"))\
                                .select("sn",F.col("col.model").alias("model_name") )\
                                .distinct()
        
        self.df_dg = spark.read.parquet( self.deviceGroup_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\

        self.df_owl = spark.read.parquet( self.owl_path )\
                            .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
                            .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
                            .filter(exclude_time_condition)        """

        self.df_sh = spark.read.parquet(self.station_history_path)\
                            .filter(exclude_time_condition)

        self.df_sh.groupby(F.hour(F.col("datetime"))).count().show()
    

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
                                        count(  when(  (col("action") == "2")&( F.col("sta_type") == "2" ) , True)).alias("band_success_count"),
                                    )

        df_apsteer = df_sh.select( "sn",
                                    col("Diag_Result_ap_steer.sta_type").alias("sta_type"),
                                    col("Diag_Result_ap_steer.action").alias("action"),
                                    )\
                            .filter(  ( F.col("sta_type") == "2" )  )
        
        df_apsteer = df_apsteer.groupBy("sn")\
                                .agg(
                                    count(  when(  (col("action") == "2")&( F.col("sta_type") == "2" ) , True)).alias("ap_success_count"),
                                )


        df_distinct_rowkey_count = df_sh.groupBy("sn")\
                                        .agg( F.countDistinct("rowkey").alias("distinct_rowkey_count")  )

        son_df = df_bandsteer.join(df_apsteer, on="sn", how="full_outer")\
                            .withColumn(
                                "steer_start_count",
                                F.coalesce(col("band_success_count"), lit(0)) + F.coalesce(col("ap_success_count"), lit(0))
                            )\
                            .withColumn(
                                        "steer_start_category",
                                        F.when(F.col("steer_start_count") > 60, "Poor")
                                        .when((F.col("steer_start_count") >= 31) & (F.col("steer_start_count") <= 60), "Fair")
                                        .when((F.col("steer_start_count") >= 11) & (F.col("steer_start_count") <= 30), "Good")
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
        df_rowkey_rssi_category.show(50, truncate = False)
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
        df_rowkey_phyrate_category.show(50, truncate = False)
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
                                        .withColumn("stationarity", when( col("diff")<= 5, lit("1")).otherwise( lit("0") ))\
                                        .groupby(partition_columns).agg(max("stationarity").alias("stationarity"))\
                                        .filter( col("stationarity")>0 )

        df_ts_station = df_sh.join( stationary_daily_df, ["sn","rowkey"])\
                                .select( "sn","rowkey", "datetime", col("Station_Data_connect_data.station_mac").alias("station_mac") )\
                                .filter( col("station_mac").isNotNull() )\
                                .groupBy("sn","datetime")\
                                .agg(  F.count("station_mac").alias("station_cnt") )

        window_spec = Window.partitionBy("sn").orderBy("datetime")
        df_lagged = df_ts_station.withColumn( "prev_station_cnt",  F.lag("station_cnt").over(window_spec) )\
                                .withColumn( "drop_diff", F.col("prev_station_cnt") - F.col("station_cnt") )\
                                .filter(  F.col("drop_diff") > 3 )
        df_lagged.show(50, truncate = False)
        df_result = df_lagged.groupBy("sn")\
                            .agg( F.count("drop_diff").alias("no_sudden_drop") )\
                            .withColumn( "sudden_drop_category", 
                                        F.when(F.col("no_sudden_drop") >= 2, "Poor")\
                                        .when( F.col("no_sudden_drop") == 1, "Fair")\
                                        .when( F.col("no_sudden_drop").isNull(), "Excellent")\
                                        .otherwise("Good")
                                    )

        return df_result

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Zhe_wifiscore_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'

    date_val = (date.today() - timedelta(7) )
    analysis = wifiKPIAnalysis(date_val=date_val)
    analysis.load_data()
    analysis.calculate_steer().show()
    analysis.calculate_rssi().show()
    analysis.calculate_phyrate().show()
    analysis.calculate_sudden_drop().show()

    """
    day_before = 5
    date_val = ( date.today() - timedelta(day_before) )
    date_compact = date_val.strftime("%Y%m%d") # e.g. 20231223
    deviceGroup_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/{ (date_val+timedelta(1)).strftime('%Y%m%d')  }"
    owl_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/OWLHistory/{ (date_val+timedelta(1)).strftime('%Y%m%d')  }"
    stationhist_path = f"{hdfs_pd}/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/{(date_val+timedelta(1)).strftime('%Y%m%d') }"

    cache_file = "/user/ZheS/wifi_score_v4/cache"
    spark.read.parquet( stationhist_path )\
        .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
        .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
        .filter(col("sn")=="ABJ22300081")\
        .write.mode("overwrite").parquet(f"{hdfs_pd}{cache_file}/StationHistory/{(date_val).strftime('%Y-%m-%d')}")


    spark.read.parquet( deviceGroup_path )\
        .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)', 1))\
        .filter(col("sn")=="ABJ22300081")\
        .write.mode("overwrite").parquet(f"{hdfs_pd}/{cache_file}/DeviceGroups/{(date_val).strftime('%Y-%m-%d')}")
    
    spark.read.parquet( owl_path )\
        .withColumn("sn", F.regexp_extract(F.col("rowkey"), r'-(\w+)_', 1))\
        .withColumn("datetime", F.from_unixtime(F.col("ts") / 1000).cast("timestamp"))\
        .filter(col("sn")=="ABJ22300081")\
        .write.mode("overwrite").parquet(f"{hdfs_pd}{cache_file}/OWLHistory/{(date_val).strftime('%Y-%m-%d')}")
    """