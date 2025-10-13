from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, concat_ws, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys 
sys.path.append('/usr/apps/vmas/scripts/ZS') 
from MailSender import MailSender
import argparse 

import subprocess
def check_hdfs_files(path, filename):
    # check if file exist
    ls_proc = subprocess.Popen(['/usr/apps/vmas/hadoop-3.3.6/bin/hdfs', 'dfs', '-du', path], stdout=subprocess.PIPE)

    ls_proc.wait()
    # check return code
    ls_lines = ls_proc.stdout.readlines()
    all_files = []
    for i in range(len(ls_lines)):
        all_files.append(ls_lines[i].split()[-1].decode('utf-8').split('/')[-1])

    if filename in all_files:
        return True
    return False

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

def SH_process(dfsh):
    # process the columns so that we can get what we want from Station History data
    dfsh_new = dfsh.withColumn('rowkey_length', F.length('rowkey'))\
                    .withColumn('rk_sn_row', F.col('rowkey').substr(F.lit(6), F.col('rowkey_length') - F.lit(18)))\
                    .withColumn('rk_sn', F.col('rowkey').substr(F.lit(6), F.col('rowkey_length') - F.lit(22)))\
                    .withColumn('rk_sn_length', F.length('rk_sn'))\
                    .withColumn('rk_row', F.col('rowkey').substr(F.lit(1),F.lit(4)))\
                    .withColumn("rk_row_sn",concat_ws("-","rk_row","rk_sn_row"))\
                    .withColumn('sdcd_tlr_length', F.length('Station_Data_connect_data_b_tx_link_rate'))\
                    .withColumn('sdcd_tx_link_rate', F.col('Station_Data_connect_data_b_tx_link_rate').substr(F.lit(1), F.col('sdcd_tlr_length') - F.lit(4)))\
                    .withColumn("sdcd_tx_link_rate",col("sdcd_tx_link_rate").cast("long"))\
                    .withColumn('sdcd_lr_length', F.length('Station_Data_connect_data_b_link_rate'))\
                    .withColumn('sdcd_link_rate', F.col('Station_Data_connect_data_b_link_rate').substr(F.lit(1), F.col('sdcd_lr_length') - F.lit(4)))\
                    .withColumn("sdcd_link_rate",col("sdcd_link_rate").cast("long"))\
                    .withColumn('sdcd_connect_type', F.col('Station_Data_connect_data_b_connect_type').substr(F.lit(1), F.lit(2)))\
                    .withColumn("sdcd_signal_strength", F.col("Station_Data_connect_data_b_signal_strength").cast("long"))\
                    .withColumn("byte_send", F.col("Station_Data_connect_data_b_diff_bs").cast("long"))\
                    .withColumn("byte_received", F.col("Station_Data_connect_data_b_diff_br").cast("long"))
    return dfsh_new
    
def DG_process(dfdg):
    # get and process the columns we need from Device Group data
    dfdg_out = dfdg.select(col("rowkey").alias("dg_rowkey")
                            ,col("col_b_mac").alias("RouExt_mac")
                            ,col("col_b_model").alias("dg_model")
                            ,col("col_b_parent_mac").alias("parent_mac")
                            ,col("col_b_fw").alias("firmware"))\
                    .groupby("dg_rowkey","RouExt_mac","dg_model")\
                        .agg(max("parent_mac").alias("parent_mac")
                             ,max("firmware").alias("firmware"))
    return dfdg_out
    
def get_Home(dfsh):
    # get required columns and filter connect_type to have only wireless data
    dfsh_out = dfsh.filter((dfsh.sdcd_connect_type.isin(['2.','5G','6G'])))\
                    .select("rowkey",
                            col("rk_sn_row").alias("serial_num"),
                            "rk_row_sn",
                            "rk_row",
                            col("Station_Data_connect_data_b_station_mac").alias("station_mac"),
                            col("Station_Data_connect_data_b_parent_id").alias("parent_id"),
                            "sdcd_signal_strength",
                            "sdcd_tx_link_rate",
                            "sdcd_link_rate",
                            "sdcd_connect_type",
                            "byte_send",
                            "byte_received"
                            )\
                    .dropna(subset="sdcd_signal_strength")\
                    .dropna(subset="sdcd_tx_link_rate")
    
    return dfsh_out

def get_phyrate(dfsh):
    # get all rowkey_related, station_mac, parent id, phy_rate --> new table
    dfsh_out = dfsh.filter((dfsh.sdcd_tx_link_rate>6)
                            & (dfsh.sdcd_tx_link_rate<2500) 
                            & (~dfsh.sdcd_tx_link_rate.isin([12,24])))\
                    .select("rowkey"
                            ,"serial_num"
                            ,"rk_row_sn"
                            ,"rk_row"
                            ,"station_mac"
                            ,"parent_id"
                            ,"sdcd_tx_link_rate"
                            ,"sdcd_connect_type").dropna(subset="sdcd_tx_link_rate")
    return dfsh_out

def round_columns(df,numeric_columns = None, decimal_places=4):  
    """  
    Rounds all numeric columns in a PySpark DataFrame to the specified number of decimal places. 
    Parameters: 
        df (DataFrame): The PySpark DataFrame containing numeric columns to be rounded.  
        decimal_places (int, optional): The number of decimal places to round to (default is 2).  
    Returns:  
        DataFrame: A new PySpark DataFrame with numeric columns rounded to the specified decimal places.  
    """  
    from pyspark.sql.functions import round  
    # List of numeric column names  
    if numeric_columns is None:
        numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == "double" or data_type == "float"]  

    # Apply rounding to all numeric columns  
    for col_name in numeric_columns:  
        df = df.withColumn(col_name, round(col(col_name), decimal_places))  
    return df 

class wifiScore():
    global hdfs_pd, station_history_path, device_groups_path,serial_mdn_custid, device_ids
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    device_groups_path = hdfs_pd + "/usr/apps/vmas/sha_data/bhrx_hourly_data/DeviceGroups/"
    serial_mdn_custid = hdfs_pd + "/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv"
    device_ids = ["rowkey","rk_row_sn","serial_num","station_mac"]
    
    def __init__(self, 
                day,
                df_stationHist
            ) -> None:
        self.date_str1 = day.strftime("%Y%m%d") # e.g. 20231223
        self.date_str2 = day.strftime("%Y-%m-%d") # e.g. 2023-12-23
        self.df_stationHist = df_stationHist

        self.stationarity = self.filter_outlier()
        
        self.parent_id = self.df_stationHist.select( device_ids + ["parent_id"]).distinct()

        self.df_rssi = self.get_rssi_dataframe()
        self.df_phy = self.get_phyrate_dataframe()
        self.df_phy_rssi = self.df_phy.drop('poor_count', 'total_count')\
                                    .join( self.df_rssi.drop('count_cat1', 'count_cat2', 'count_cat3', 'total_count'), 
                                            device_ids)\
                                    .withColumn( 
                                                "weights", 
                                                when( 
                                                    (col("avg_phyrate") < 20.718) & (col("stationarity") == 1), 
                                                    col("weights") * col("avg_phyrate") / 20.718 
                                                ).otherwise(col("weights")) 
                                                ) 
        self.df_all_features = self.add_info()
        self.df_deviceScore = self.create_deviceScore()
        
        self.df_homeScore = self.df_deviceScore.groupBy("serial_num", "mdn", "cust_id")\
                    .agg(  
                        count("*").alias("num_station"),    
                        F.round(F.sum(col("poor_rssi") * col("weights")), 4).alias("poor_rssi"),  
                        F.round(F.sum(col("poor_phyrate") * col("weights")), 4).alias("poor_phyrate"),  
                        F.round(F.sum(col("device_score") * col("weights")), 4).alias("home_score"), 
                        F.collect_set("dg_model").alias("dg_model"), 
                        F.collect_set("Rou_Ext").alias("Rou_Ext"), 
                    )\
                    .withColumn("date", F.lit( ( datetime.strptime(self.date_str2,"%Y-%m-%d") - timedelta(1) ).strftime("%Y-%m-%d") ))\
                    .select( "*",F.explode("dg_model").alias("dg_model_mid") ).dropDuplicates()\
                    .select( "*",F.explode("dg_model_mid").alias("dg_model_indiv") )\
                    .drop("dg_model_mid")\
                    .dropDuplicates()
    
    def create_deviceScore(self,df_all_features = None):
        if df_all_features is None:
            df_all_features = self.df_all_features

        windowSpec = Window.partitionBy('serial_num',"mdn","cust_id") 
        sum_weights = F.sum('weights').over(windowSpec) 

        df_deviceScore = df_all_features.withColumn( 
                                                    "device_score", 
                                                    (100 - col("poor_rssi")) * 0.5 + (100 - col("poor_phyrate")) * 0.5
                                                )\
                                        .withColumn('weights', F.col('weights') / sum_weights)\
                                        .drop("dg_rowkey")
        return df_deviceScore
     
    def filter_outlier(self, df = None, partition_columns = None, percentiles = None, column_name = None):
        if df is None:
            df = self.df_stationHist 
        if partition_columns is None:
            partition_columns = ["rowkey","rk_row_sn","station_mac","serial_num"]
        if percentiles is None:
            percentiles = [0.03, 0.1, 0.5, 0.9]
        if column_name is None:
            column_name = "sdcd_signal_strength"

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

        df_stationary = df_outlier.withColumn("diff", col('90%_val') - col('50%_val') )\
                        .withColumn("stationarity", when( col("diff")<= 5, lit("1")).otherwise( lit("0") ))\
                        .groupby(["rowkey","rk_row_sn","serial_num","station_mac"]).agg(max("stationarity").alias("stationarity"))

        return df_stationary

    def get_rssi_dataframe(self, df_stationHist = None):
        
        if df_stationHist is None:
            df_stationHist = self.df_stationHist
            
        df_sdcd = df_stationHist.drop("sdcd_tx_link_rate","sdcd_link_rate")\
                            .dropna(subset="sdcd_signal_strength")
    
        condition_cat1 = (col("sdcd_connect_type") == "2.") & (col("sdcd_signal_strength") < -78) 
        condition_cat2 = (col("sdcd_connect_type") == "5G") & (col("sdcd_signal_strength") < -75) 
        condition_cat3 = (col("sdcd_connect_type") == "6G") & (col("sdcd_signal_strength") < -70) 
        
        total_volume_window = Window.partitionBy("serial_num") 
        
        df_rssi = ( 
                df_sdcd.groupBy(device_ids ) 
                .agg( 
                    sum(when(condition_cat1, 1).otherwise(0)).alias("count_cat1"), 
                    sum(when(condition_cat2, 1).otherwise(0)).alias("count_cat2"), 
                    sum( when(condition_cat3, 1).otherwise(0) ).alias("count_cat3"), 
                    # Calculate the average signal strength for each condition 
                    F.avg(F.when((col("sdcd_connect_type") == "2."), F.col("sdcd_signal_strength")).otherwise(None)).alias("avg_sig_strength_cat1"), 
                    F.avg(F.when((col("sdcd_connect_type") == "5G"), F.col("sdcd_signal_strength")).otherwise(None)).alias("avg_sig_strength_cat2"), 
                    F.avg(F.when((col("sdcd_connect_type") == "6G"), F.col("sdcd_signal_strength")).otherwise(None)).alias("avg_sig_strength_cat3"),        
                    
                    count("*").alias("total_count"),
                    sum("byte_send").alias("byte_send"),
                    sum("byte_received").alias("byte_received")
                ) 
                .filter(col("total_count")>=36)
                .withColumn("volume",F.log( col("byte_send")+col("byte_received") ))
                .withColumn("total_volume", F.sum("volume").over(total_volume_window)) 
                .withColumn("weights", F.col("volume") / F.col("total_volume") ) 
                .withColumn("poor_rssi", (col("count_cat1") + col("count_cat2") + col("count_cat3"))/col("total_count") *100 )
                .join( self.stationarity, ["rowkey","rk_row_sn","serial_num","station_mac"], "inner")
            ) 
            
        return df_rssi
        
    def get_phyrate_dataframe(self, df_stationHist = None):
        if df_stationHist is None:
            df_stationHist = self.df_stationHist
        
        # get phyrate
        condition = col("sdcd_tx_link_rate") < 65
        
        df_phy = df_stationHist.transform(get_phyrate)\
                            .groupby( device_ids )\
                            .agg( 
                                avg("sdcd_tx_link_rate").alias("avg_phyrate"), 
                                sum(when(condition, 1).otherwise(0)).alias("poor_count"), 
                                count("*").alias("total_count")
                                )\
                            .withColumn("poor_phyrate", col("poor_count")/col("total_count")*100 )
        return df_phy

    def add_info(self, df_phy_rssi = None, date_str1 = None, date_str2 = None):
        if df_phy_rssi is None:
            df_phy_rssi = self.df_phy_rssi.join( self.parent_id, device_ids )
        if date_str1 is None:
            date_str1 = self.date_str1
        if date_str2 is None:
            date_str2 = self.date_str2
        
        # add mdn and cust_id
        days_before = [1, 2, 3, 4, 5] 
        for days_ago in days_before: 
            try:
                d = ( datetime.strptime(date_str2,"%Y-%m-%d") - timedelta(days_ago) ).strftime("%Y-%m-%d")
                #p = hdfs_pd +"/usr/apps/vmas/5g_data/fixed_5g_router_mac_sn_mapping/{}/fixed_5g_router_mac_sn_mapping.csv".format(d)
                p = serial_mdn_custid.format(d)
                df_join = spark.read.option("header","true").csv(p)\
                            .filter(col('status')=="INSTALL COMPLETE" )\
                            .select( col("mdn_5g").alias("mdn"),
                                    col("serialnumber").alias("serial_num"),
                                    "cust_id"
                                    )
                break
            except:
                pass
        df_all = df_join.join( df_phy_rssi, "serial_num", "right" )
        
        # add device group information and Router/Extender
        days_before = [0, 1, 2, 3, 4, 5] 
        for days_ago in days_before: 
            try:
                d = ( datetime.strptime(date_str1,"%Y%m%d") - timedelta(days_ago) ).strftime("%Y%m%d")
                device_groups_path1 = device_groups_path + d
                
                dfdg = spark.read.parquet(device_groups_path1)\
                                        .select("rowkey",explode("Group_Data_sys_info"))\
                                        .transform(flatten_df_v2)\
                                        .transform(DG_process)
                break
            except:
                pass
                                
        cond = [dfdg.dg_rowkey==df_all.rk_row_sn, dfdg.RouExt_mac==df_all.parent_id]
        df_mac =  dfdg.join(df_all,cond,"right")\
                        .withColumn("Rou_Ext",when( col("parent_mac").isNotNull(),1 ).otherwise(0) )
    
        group_id = [ "serial_num", "mdn", "cust_id", "rowkey", "rk_row_sn", "station_mac"] 
        rou_ext = ["RouExt_mac", "dg_model", "parent_mac", "firmware", "parent_id", "Rou_Ext"] 
        features = ["avg_phyrate", "poor_phyrate", "stationarity","volume", "weights", "poor_rssi", 
                    "avg_sig_strength_cat1","avg_sig_strength_cat2","avg_sig_strength_cat3"] 
        #features = ["avg_phyrate", "poor_phyrate", "weights", "poor_rssi"] 
        
        aggregation_exprs = [F.avg(col(feature)).alias(feature) for feature in features] + \
                            [F.collect_set(col(col_name)).alias(col_name) for col_name in rou_ext]
        
        result_df = df_mac.groupBy(group_id).agg(*aggregation_exprs) 
        
        return result_df
            

        
if __name__ == "__main__":
    spark = SparkSession.builder.appName('Zhe_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()
    hdfs_pa = 'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    date_str = (date.today() - timedelta(1)).strftime("%Y-%m-%d")
    from pyspark.sql.functions import col, from_unixtime, date_format

    df_sh = spark.read.parquet("/usr/apps/vmas/sha_data/bhrx_hourly_data/StationHistory/20241218")\
        .select( "rowkey", "ts","Tplg_Data_model_name",
                        col("Station_Data_connect_data.station_mac").alias("station_mac"),
                        col("Station_Data_connect_data.connect_type").alias("connect_type"), 
                        col("Station_Data_connect_data.diff_bs").alias("byte_send"), 
                        col("Station_Data_connect_data.diff_br").alias("byte_received"), 
                            )\
        .withColumn("sn", F.regexp_extract("rowkey", r"([A-Z]+\d+)", 1))\
        .withColumn(
                "connect_type",
                F.when(F.col("connect_type").like("2.4G%"), "2_4G")
                .when(F.col("connect_type").like("5G%"), "5G")
                .when(F.col("connect_type").like("6G%"), "6G")
                .otherwise(F.col("connect_type"))  
            )\
        .filter( col("Tplg_Data_model_name").isin(["CR1000B","CR1000A"]) )\
        .groupby("sn","rowkey","connect_type")\
        .agg( F.sum(  col("byte_send")).alias("byte_send") ,
            F.sum(  col("byte_received")).alias("byte_received")
            )

    path = f"/usr/apps/vmas/bhrx/{date_str}/bhrx_fsam_flume"
    df_fsam = spark.read.parquet(path)\
                        .select( 'rowkey','sn','device_group','device_class','name_to_display','suggested_name' ).distinct()
    

    """
    df_location = spark.read.option("header", "true").csv( hdfs_pa + "/fwa/order_oracle")\
                        .select("MDN",col("GEOCODED_ADDRESS"))

    df_sh.join( spark.read.parquet( f"/user/ZheS/wifi_score_v3/archive/{date_str}" ).select('serial_num',"mdn"), "serial_num" )\
        .join( df_location,"mdn" )\
        .withColumn("state", F.regexp_extract("GEOCODED_ADDRESS", r"\b([A-Z]{2}) \d{5}", 1))\
        .join(df_fsam, "rowkey")\
        .write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/fsam/deviceGroup_hour_result")


    """
    df_wifi = spark.read.parquet( "/user/ZheS/wifi_score_v4/KPI/2024-12-18" )\
                    .select("sn","model_name","wifiScore")\
                    .filter( col("model_name").isin(["CR1000B","CR1000A"]) )
    df_fsam.join(df_sh, "rowkey")\
        .join(df_wifi,"sn")\
        .filter(col("connect_type").isin(["2_4G","5G"]))\
        .groupby("wifiScore","connect_type")\
        .agg( F.sum(  col("byte_send")+col("byte_received") ).alias("data_volume") )\
        .orderBy("wifiScore","connect_type")\
        .show()
        #.write.mode("overwrite").parquet(f"{hdfs_pd}/user/ZheS/wifi_score_v4/fsam/connect_type_result")
