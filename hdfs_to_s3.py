from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lag, col, split, concat_ws, lit ,udf,count, max,lit,avg, when,concat_ws,to_date,explode
from pyspark.sql.types import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import subprocess
import traceback

def rename_hdfs_file(source_path, target_path):
    """
    Rename a file in HDFS.

    Args:
        source_path (str): The current HDFS path of the file, including the file name.
        target_path (str): The new HDFS path of the file, including the new file name.
    """
    try:
        # HDFS command to rename (move) the file
        mv_cmd = f"hdfs dfs -mv {source_path} {target_path}"
        
        # Execute the command
        process = subprocess.Popen(mv_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()
        
        # Check for errors
        if process.returncode != 0:
            print(f"Error renaming file in HDFS: {stderr.decode('utf-8')}")
        else:
            print(f"File renamed successfully from {source_path} to {target_path}")
    except Exception as e:
        print(f"Exception occurred: {str(e)}")

def check_hdfs_files(path, filename): 
    # check if file exists
    ls_proc = subprocess.Popen(
        ['/usr/apps/vmas/hadoop-3.3.6/bin/hdfs', 'dfs', '-du', path], 
        stdout=subprocess.PIPE
    ) 
    ls_proc.wait()  # check return code
    ls_lines = ls_proc.stdout.readlines() 
    all_files = []
    for i in range(len(ls_lines)): 
        all_files.append(ls_lines[i].split()[-1].decode("utf-8").split('/')[-1]) 
    if filename in all_files: 
        return True 
    return False 

def check_aws_files(path, filename): 
    # check if file exists
    path_adj = "s3" + path[3:] 
    try: 
        ls_proc = subprocess.Popen(
            ['/usr/local/bin/aws', 's3', 'ls', path_adj], 
            stdout=subprocess.PIPE
        ) 
        ls_proc.wait()  # check return code
        ls_lines = ls_proc.stdout.readlines() 
        all_files = [] 
        for i in range(len(ls_lines)): 
            all_files.append(ls_lines[i].split()[-1].decode("utf-8").split('/')[0]) 
        if filename in all_files: 
            return True 
        return False 
    except Exception as e: 
        subject = '[ISSUE]' 
        text1 = str(traceback.format_exc()) + "\n" 
        text1 = text1 + str(e) + "\n" 
        print(text1) 
        return False 

def push_hdfs_to_s3(hdfs_path, hdfs_file, s3_path, s3_file):
    """
    Push a file from HDFS to S3 if it doesn't already exist in S3.

    Args:
        hdfs_path (str): The HDFS directory path.
        hdfs_file (str): The file in HDFS to be copied.
        s3_path (str): The S3 directory path.
        s3_file (str): The file in S3 to be created.

    Returns:
        None
    """
    global text
    text = ""

    # Construct the full HDFS file path
    full_hdfs_path = f"{hdfs_path.rstrip('/')}/{hdfs_file.lstrip('/')}"

    # Base command for Hadoop DistCp
    base_cmd = (
        "/usr/apps/vmas/hadoop-3.3.6/bin/hadoop distcp "
        "-Dfs.s3a.access.key=AKIAQDQI7W3D5XKRKXRF "
        "-Dfs.s3a.secret.key=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 "
        "-Dfs.s3a.fast.upload=true -update -bandwidth 10 -m 20 -strategy dynamic "
    )

    # Check if the HDFS file exists
    if not check_hdfs_files(hdfs_path, hdfs_file):
        message = f"{full_hdfs_path} doesn't exist"
        print(message)
        text += message + "\n"
        return

    # Check if the file already exists in S3
    if check_aws_files(s3_path, s3_file):
        message = f"File {s3_file} already exists"
        print(message)
        text += message + "\n"
        return

    # Construct the full command and execute it
    full_cmd = f"{base_cmd} {full_hdfs_path}/* {s3_path.rstrip('/')}/{s3_file}/"
    try:
        process = subprocess.Popen(full_cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            message = f"Got an error when running {full_cmd} {stderr.decode('utf-8')}"
            print(message)
            text += message + "\n"
        else:
            message = f"Uploaded {s3_file} successfully"
            print(message)
            text += message + "\n"
    except Exception as e:
        message = f"Exception occurred: {str(e)}"
        print(message)
        text += message + "\n"

if __name__ == "__main__":

    spark = SparkSession.builder.appName('Zhe_wifiscore_Test')\
                        .config("spark.ui.port","24045")\
                        .getOrCreate()

    hdfs_pd = "hdfs://njbbvmaspd11.nss.vzwnet.com:9000/"
    hdfs_pa =  'hdfs://njbbepapa1.nss.vzwnet.com:9000'
    parquet_file = "/user/ZheS/wifi_score_v4/KPI/2024-09-25"
    output_path = "/user/ZheS/wifi_score_v4/sample/"
    target_path = "/user/ZheS/wifi_score_v4/sample/2024-09-25.csv"

    hdfs_path = hdfs_pd+"//user/ZheS/wifi_score_v4//"
    hdfs_file = "sample"
    s3_path = "s3a://prod-bhr-backup/whw_data/sha_ml_dt_wifi_score/"
    s3_file = "2024-09-25"

    models = ['ASK-NCQ1338', 'ASK-NCQ1338FA', 'WNC-CR200A', "CR1000A","CR1000B"]

    spark.read.parquet(parquet_file)\
        .filter( F.col("model_name").isin( models ) )\
        .coalesce(1).write.csv(output_path, header=True, mode = "overwrite")
        
    import subprocess
    import os

    result = subprocess.run(["hdfs", "dfs", "-ls", output_path], capture_output=True, text=True)
    file_list = result.stdout.splitlines()

    csv_file = None
    for line in file_list:
        if line.endswith(".csv"):
            csv_file = line.split()[-1]
        elif line.endswith("_SUCCESS"):
            success_file = line.split()[-1]

    if success_file:
        subprocess.run(["hdfs", "dfs", "-rm", success_file])
    print(f"CSV file: {csv_file}")

    rename_hdfs_file(csv_file, target_path)


    print( check_hdfs_files(hdfs_path, hdfs_file) )
    print( check_aws_files(s3_path, s3_file) )


    push_hdfs_to_s3(hdfs_path, hdfs_file, s3_path, s3_file)
    print( check_aws_files(s3_path, s3_file) )