from __future__ import print_function
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
spark = SparkSession.builder.appName('Kmeans').config('spark.default.parallelism',5).getOrCreate()

df = spark.read.csv(sys.argv[1],inferSchema=True,header=True)
df = df.select([col(c).cast(FloatType()) for c in df.columns])
df = df.na.fill(0.0)
print(df.dtypes)
df.show()
data = df.rdd.map(lambda x:(Vectors.dense(x[:-1]),x[-1])).toDF(["features", "label"])
REG = 0.1
lr = LogisticRegression(regParam=0.1)
lrModel = lr.fit(data)
res_train = lrModel.transform(data)
res_train.select("label","probability", "prediction").show(2000)
print("Finding the most important features:")
df.printSchema()
print("Multinomial coefficients: " + str(lrModel.coefficientMatrix))
res = df.select([mean(c) for c in df.columns]).head(1)
for a in res:
	print(a)
print('-'*20)
