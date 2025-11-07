
#!/bin/bash 

export HADOOP_HOME=/usr/apps/vmas/hadoop-3.3.6
export SPARK_HOME=/usr/apps/vmas/spark
export PYSPARK_PYTHON=/usr/apps/vmas/anaconda3/bin/python3 

echo "RES: Starting==="
echo $(date)


# -------------------------------------------------
# Run Spark job
# -------------------------------------------------
/usr/apps/vmas/spark/bin/spark-submit \
  --master spark://njbbepapa1.nss.vzwnet.com:7077 \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.driver.maxResultSize=10g" \
  --conf "spark.dynamicAllocation.enabled=false" \
  --num-executors 100 \
  --executor-cores 2 \
  --total-executor-cores 200 \
  --executor-memory 10g \
  --driver-memory 32g \
  --packages org.apache.spark:spark-avro_2.12:3.2.0,io.streamnative.connectors:pulsar-spark-connector_2.12:3.2.0.2 \
  /usr/apps/vmas/script/ZS/HourlyScore/Rolling_window_score/station_score_rolling_window.py \
  > /usr/apps/vmas/script/ZS/HourlyScore/Rolling_window_score/station_score_rolling_window.log

 

 
