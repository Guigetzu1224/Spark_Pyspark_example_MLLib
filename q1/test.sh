#!/bin/bash
source ../env.sh
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 ./q1.py hdfs://$SPARK_MASTER:9000/q1/input/ hdfs://$SPARK_MASTER:9000/q1/input_test/test.csv hdfs://$SPARK_MASTER:9000/q1/input_test/test_labels.csv
