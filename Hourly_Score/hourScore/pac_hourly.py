from datetime import datetime, timedelta, date
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructType,
    Row,
)

# Optional but common imports you had:
import numpy as np
import pandas as pd
import sys 
import time


sys.path.append('/usr/apps/vmas/script/ZS/HourlyScore') 
from StationConnection import StationConnectionProcessor, LogTime
from station_score_hourly import station_score_hourly
from wifi_score_hourly import wifi_score_hourly


def replace_rows(base_df: DataFrame, update_df: DataFrame, key_col: str = "sn") -> DataFrame:
    """
    Replace rows in base_df with rows from update_df when key_col matches.

    Args:
        base_df (DataFrame): Original dataframe (full but possibly outdated).
        update_df (DataFrame): Dataframe containing newer rows (partial).
        key_col (str): Column name to use as the unique identifier.

    Returns:
        DataFrame: Combined dataframe with rows from update_df replacing base_df.
    """
    # customers to update
    update_keys = update_df.select(key_col).distinct()

    # keep only rows in base_df that are not in update_df
    base_remaining = base_df.join(update_keys, on=key_col, how="left_anti")

    # union the updated rows
    final_df = base_remaining.unionByName(update_df)

    return final_df



if __name__ == "__main__":
    spark = SparkSession.builder.appName('station_score_hourly_bhr_wifi_score_hourly_report_v1')\
                        .config("spark.ui.port","24046")\
                        .getOrCreate()

    
    current_datetime = datetime.now()

    date_str = current_datetime.strftime("%Y%m%d")
    hour_str = current_datetime.strftime("%H")
    #station_connection_hourly
    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    station_history_path = f"{hdfs_pa}/sha_data/purple_prod/bhrx_stationhistory/"
    device_groups_path = f"{hdfs_pa}/sha_data/purple_prod//bhrx_devicegroups/"

    station_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/station_score_hourly_pac/"
    wifi_score_output_path = f"{hdfs_pa}/sha_data/hourlyScore_include_pac/wifi_score_hourly_pac/"




    with LogTime() as timer:
        processor = StationConnectionProcessor(
            spark,
            input_path= station_history_path,
            output_path= None,
            date_str= date_str,
            hour_str= hour_str
        )
        station_connection_df = processor.run()


        ins = station_score_hourly(
                                spark,
                                date_str,
                                hour_str,
                                station_connection_df,
                                output_path = station_score_output_path
                                )

        ins.run()

        wifi_score_runner = wifi_score_hourly(
                            spark=spark,
                            date_str=date_str,
                            hour_str=hour_str,
                            source_df=station_connection_df,
                            station_history_path=station_history_path,
                            device_groups_path=device_groups_path,
                            output_path=wifi_score_output_path
                                )
        wifi_score_runner.run()


        