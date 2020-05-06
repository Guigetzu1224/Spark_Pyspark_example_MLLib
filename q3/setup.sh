/usr/local/hadoop/bin/hdfs dfs -rm -r /q3/
/usr/local/hadoop/bin/hdfs dfs -mkdir /q3/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /q3/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data.csv /q3/input/data.csv
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /q3/input_test/ 
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data_test2.csv /q3/input_test/data_test.csv


