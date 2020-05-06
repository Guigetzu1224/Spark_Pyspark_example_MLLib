/usr/local/hadoop/bin/hdfs dfs -rm -r /q2/
/usr/local/hadoop/bin/hdfs dfs -mkdir /q2/
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /q2/input/
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal ./data.csv /q2/input/data.csv

