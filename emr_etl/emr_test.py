from pyspark.sql import SparkSession
# import boto3
import json

spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

#s3_resource = boto3.resource('s3')
#obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
#data = obj.get()['Body'].read().decode()


#df = spark.read.json('s3://ddapi.data/modelDataFrame.json')
#data = json.loads(data)

#df = spark.createDataFrame(Row(**x) for x in data)
print('complete')

# df = df.toPandas()
# json = df.to_json(orient='records')
# s3_bucket = s3_resource.Bucket('ddapi.data')
# s3_bucket.put_object(Body=json, Key='emr_test.json')

# df.write.parquet("s3a://ddapi.data/test.parquet", mode="overwrite")
