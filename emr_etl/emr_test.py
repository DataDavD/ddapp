from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import json
import requests
import pandas as pd

sc = SparkContext(appName="ddapp_test")

spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

# s3_resource = boto3.resource('s3')
# obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
# data = obj.get()['Body'].read().decode()


SEASON_1718 = 'https://pkgstore.datahub.io/sports-data/' \
              'english-premier-league/season-1718_json/data/' \
              'dbd8d3dc57caf91d39ffe964cf31401b/season-1718_json.json'

content_1718 = requests.get(SEASON_1718)
json1718 = json.loads(content_1718.content)

df_1718 = spark.createDataFrame(Row(**x) for x in json1718)

#df_1718 = spark.read.load('s3://ddapi.data/season-1718.csv', format='csv')
df_1819 = spark.read.load('s3://ddapi.data/season-1819.csv', format='csv')
df_1718_2 = df_1718.toPandas()
df_1819_2 = df_1819.toPandas()
#df_out = df_1819.head()

# df = spark.read.json('s3://ddapi.data/modelDataFrame.json')
# df = spark.read.load('s3://ddapi.data/modelDataFrame.csv', format='csv')

# trainData, testData = df.randomSplit([0.75, 0.25], seed=12345)

# df = spark.read.json('s3://ddapi.data/modelDataFrame.json')
# data = json.loads(data)

#d f = spark.createDataFrame(Row(**x) for x in data)
# print('complete')

# df = df.toPandas()
# json = df.to_json(orient='records')
# s3_bucket = s3_resource.Bucket('ddapi.data')
# s3_bucket.put_object(Body=json, Key='emr_test.json')

# df.write.parquet("s3://ddapi.data/test123.parquet", mode="overwrite")
df_1718.write.save("s3://ddapi.data/testout.csv", format="csv", header=True, mode="overwrite")
