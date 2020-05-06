#!/bin/bash
source ../env.sh
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q2.py hdfs://$SPARK_MASTER:9000/q2/input/
