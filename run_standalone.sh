#!/bin/bash

export SPARK_LOCAL_IP=127.0.0.1

/opt/spark/bin/spark-submit \
  --name standalone-tests \
  --master local[4] \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.ui.bindAddress=127.0.0.1 \
  --conf spark.ui.port=4090 \
  --conf spark.port.maxRetries=50 \
  /home/marin/M1/Big-Data/standalone_tests.py
