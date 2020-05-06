from __future__ import print_function
import numpy 
from pyspark.sql import SparkSession
import pandas as pd
import sys
from pyspark import SparkContext, SparkConf
from pyspark.ml.linalg import Vectors
import pyspark.sql.types as T
from pyspark.ml.feature import Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
from pyspark.sql import SQLContext
sc = SparkSession.builder.appName('Kmeans').config('spark.default.parallelism',5).getOrCreate()
sqlContext = SQLContext(sc)
df = sc.read.csv(sys.argv[1],inferSchema=True,header=True)
df = df.na.fill('0')
df_test = sc.read.csv(sys.argv[2],inferSchema=True,header=True)
df_test_labels = sc.read.csv(sys.argv[3],inferSchema=True,header=True)
df_test = df_test.join(df_test_labels, df_test.id == df_test_labels.id)

df = df.withColumn("toxic",df["toxic"].cast(IntegerType()))
df = df.withColumn("severe_toxic",df["severe_toxic"].cast(IntegerType()))
df = df.withColumn("obscene",df["obscene"].cast(IntegerType()))
df = df.withColumn("threat",df["threat"].cast(IntegerType()))
df = df.withColumn("insult",df["insult"].cast(IntegerType()))
df = df.withColumn("identity_hate",df["identity_hate"].cast(IntegerType()))

df_test = df_test.withColumn("toxic",df_test["toxic"].cast(IntegerType()))
df_test = df_test.withColumn("severe_toxic",df_test["severe_toxic"].cast(IntegerType()))
df_test = df_test.withColumn("obscene",df_test["obscene"].cast(IntegerType()))
df_test = df_test.withColumn("threat",df_test["threat"].cast(IntegerType()))
df_test = df_test.withColumn("insult",df_test["insult"].cast(IntegerType()))
df_test = df_test.withColumn("identity_hate",df_test["identity_hate"].cast(IntegerType()))
df_test.show()
out_cols = [i for i in df.columns if i not in ["id", "comment_text"]]

for col in out_cols:
	df = df.fillna({col:'0'})
	df_test = df.fillna({col:'0'})
df.show()
tokenizer = Tokenizer(inputCol="comment_text", outputCol="words")
wordsData = tokenizer.transform(df)
wordsData_test = tokenizer.transform(df_test)
# Count the words in a document
hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures")
tf = hashingTF.transform(wordsData)
tf_test = hashingTF.transform(wordsData_test)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(tf) 
idfModel_train = idf.fit(tf_test)
tfidf = idfModel.transform(tf)
tfidf_test = idfModel_train.transform(tf_test)

tfidf.select("features").first()
tfidf_test.select('features').first()
REG = 0.1

test_res = df_test.select('id')
test_res.head()
tot_corr=0
zeros = 0
ones = 0
for col in out_cols:
	lr = LogisticRegression(featuresCol="features", labelCol=col, regParam=REG)
	lrModel = lr.fit(tfidf.limit(3000))
	res_train = lrModel.transform(tfidf_test.limit(3000))
	to_join = res_train.select('id','prediction')
	to_join = to_join.withColumnRenamed('prediction',col)
	res_train.registerTempTable("final_results")
	test_res = test_res.join(to_join, on="id",how="inner" )
	res_train = res_train.withColumn("prediction",res_train["prediction"].cast(IntegerType()))
	res_train.show()
	sqlDF = sqlContext.sql('SELECT * FROM final_results WHERE final_results.prediction = final_results.'+col)
	tot_corr += sqlDF.count() 
	zeros += sqlContext.sql('SELECT * FROM final_results WHERE final_results.' + col + ' = 0').count()
	ones += sqlContext.sql('SELECT * FROM final_results WHERE final_results.' + col + ' =1').count()
res_train.show()
print('-'*1000)
print(tot_corr)
print(zeros)
print(ones)
print("Accuracy:{acc}".format(acc=float(tot_corr)/float(zeros+ones)))
print('-'*2000)
test_res.show()

#toxic|severe_toxic|obscene|threat|insult|identity_hate
#[('id', 'string'), ('comment_text', 'string'), ('toxic', 'int'), ('severe_toxic', 'int'), ('obscene', 'int'), ('threat', 'int'), ('insult', 'int'), ('identity_hate', 'int'), ('words', 'array<string>'), ('rawFeatures', 'vector'), ('features', 'vector')]

