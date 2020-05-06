/usr/local/hadoop/bin/hdfs dfs -rm -r /q1/
/usr/local/hadoop/bin/hdfs dfs -mkdir /q1/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /q1/input/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /q1/input_test/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data/test.csv /q1/input_test/test.csv
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data/train.csv /q1/input/train.csv
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data/test_labels.csv /q1/input_test/test_labels.csv
