#!/bin/bash
source ../env.sh
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q3.py hdfs://$SPARK_MASTER:9000/q3/input/ hdfs://$SPARK_MASTER:9000/q3/input_test/
