from __future__ import print_function
import os
import numpy 
from pyspark.sql import SparkSession
import pandas as pd
import sys
from pyspark.sql.functions import col,mean
from pyspark import SparkContext, SparkConf
from pyspark.ml.linalg import Vectors
import pyspark.sql.types as T
from pyspark.sql.types import DoubleType,FloatType
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import IntegerType
from functools import reduce
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.sql import SQLContext

spark = SparkSession.builder.appName('Kmeans').config('spark.default.parallelism',5).getOrCreate()
sqlContext = SQLContext(spark)

SPARK_PATH = os.environ['SPARK_MASTER']
newColumns = ['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','income_bracket']
data = spark.read.csv("hdfs://"+SPARK_PATH+":9000/q3/input/",inferSchema=True,header=True)
oldColumns = data.schema.names
print('-'*100)
data.show()
df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), data)
df = df.drop('education')
to_index = ['workclass','marital_status','occupation','relationship','race','sex','native_country','income_bracket']
for column_name in to_index:
	stringIndexer = StringIndexer(inputCol=column_name,outputCol=column_name+"_")
	model = stringIndexer.fit(df)
	df = model.transform(df)
df = df.drop(*to_index)
df.show()
df = df.na.fill(0.0)
data = df.rdd.map(lambda x:(Vectors.dense(x[:-1]),x[-1])).toDF(["features", "label"])
REG = 0.1
lr = LogisticRegression(regParam=0.1)
lrModel = lr.fit(data)
res_train = lrModel.transform(data)
res_train.select("label","probability", "prediction").show(2000)


data_test = spark.read.csv("hdfs://"+SPARK_PATH+":9000/q3/input_test/data_test.csv",inferSchema=True,header=True)
oldColumns = data_test.schema.names
df_test = reduce(lambda data_test, idx: data_test.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), data_test)
for column_name in to_index:
        stringIndexer = StringIndexer(inputCol=column_name,outputCol=column_name+"_")
	model = stringIndexer.fit(df_test)
        df_test = model.transform(df_test)
df_test = df_test.drop('education')
df_test = df_test.drop(*to_index)
df_test.na.fill(0.0)
df_test.show()
data_test = df_test.rdd.map(lambda x:(Vectors.dense(x[:-1]),x[-1])).toDF(["features", "label"])
data_test_pred = lrModel.transform(data_test)
data_test_pred.select("label","probability", "prediction").show(2000)
#predictionAndLabels = data_test.rdd.map(lambda lp: (float(lrModel.predict(lp.features)), lp.label)).toDF(["features", "label"])
zeros = 0
ones = 0
tot_corr = 0
data_test_pred.registerTempTable("final_results")
#data_test_pred = data_test_pred.withColumn("prediction",res_train["prediction"].cast(IntegerType()))
sqlDF = sqlContext.sql('SELECT * FROM final_results WHERE final_results.prediction = final_results.label')
tot_corr += sqlDF.count()
zeros += sqlContext.sql('SELECT * FROM final_results WHERE final_results.label = 0').count()
ones += sqlContext.sql('SELECT * FROM final_results WHERE final_results.label  =1').count()
#metrics = BinaryClassificationMetrics(predictionAndLabels)
print(tot_corr)
print(ones)
print(zeros)

print("Accuracy of system: {acc}".format(acc=float(tot_corr)/float(ones+zeros)))
