from pyspark import SparkContext
from pyspark.sql import SparkSession, Row
import json
import requests

sc = SparkContext(appName="ddapp_test")

spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

SEASON_1718 = 'https://pkgstore.datahub.io/sports-data/' \
              'english-premier-league/season-1718_json/data/' \
              'dbd8d3dc57caf91d39ffe964cf31401b/season-1718_json.json'

content_1718 = requests.get(SEASON_1718)
json1718 = json.loads(content_1718.content)

df_1718 = spark.createDataFrame(Row(**x) for x in json1718)

df_1819 = spark.read.load('s3://ddapi.data/season-1819.csv', format='csv')

df_1718.write.save("s3://ddapi.data/testout.csv",
                   format="csv",
                   header=True,
                   mode="overwrite")
