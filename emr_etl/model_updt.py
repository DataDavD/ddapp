from pyspark.sql import SparkSession, Row
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np
import boto3
import json
import pyspark
from aws_cred import ACCESS_KEY, SECRET_KEY

spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

s3_resource = boto3.resource('s3')
obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
data = obj.get()['Body'].read().decode()

data = json.loads(data)

df = spark.createDataFrame(Row(**x) for x in data)
df.show()

# train and get model score (ROC AUC) using logistic regression

trainData, testData = df.randomSplit([0.75, 0.25], seed=12345)

# lets first create pipeline to transform the categorical
# and numerical columns for logistic regression

categoricalColumns = ['HomeTeam', 'AwayTeam']
cat_stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol=categoricalCol,
                                  outputCol=categoricalCol + "Index")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()],
                                     outputCols=[categoricalCol + "catVec"])
    cat_stages += [stringIndexer, encoder]


assemblerInputs = [c + "catVec" for c in categoricalColumns]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="featuresCat")
cat_stages += [assembler]

# create cat pipeline
pipelineCat = Pipeline(stages=cat_stages)

numericCols = ['homeLast5goals', 'awayLast5goals',
               'homeLast5shots_on', 'awayLast5shots_on']

assemblerNum = VectorAssembler(inputCols=numericCols,
                               outputCol="featuresNum0")
standardScalerNum = StandardScaler(inputCol="featuresNum0",
                                   outputCol="featuresNum")

# create numeric feature pipeline
pipelineNum = Pipeline(stages=[assemblerNum, standardScalerNum])

lr = LogisticRegression()

pipeline = Pipeline(stages=[pipelineCat, pipelineNum,
                            VectorAssembler(inputCols=["featuresCat",
                                                       "featuresNum"],
                                            outputCol="features"),
                            lr])

paramGrid = ParamGridBuilder()\
    .addGrid(lr.regParam, np.arange(0, 1, .1)) \
    .addGrid(lr.elasticNetParam, np.arange(0, 1, .1)) \
    .build()

evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)  # don't have much data yet

cvModel_lgreg = crossval.fit(trainData)

pred_lgreg = cvModel_lgreg.transform(testData)
score_lgreg = float(evaluator.evaluate(pred_lgreg,
                                       {evaluator.metricName: "areaUnderROC"}))
print(score_lgreg)

# train and get model score (ROC AUC) using gradient boosted tree

trainData, testData = df.randomSplit([0.8, 0.2], seed=12345)

# lets first create pipeline to transform the categorical
# and numerical columns for logistic regression

categoricalColumns = ['HomeTeam', 'AwayTeam']
cat_stages = []
for categoricalCol in categoricalColumns:
    stringIndexer = StringIndexer(inputCol=categoricalCol,
                                  outputCol=categoricalCol + "Index")
    encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()],
                                     outputCols=[categoricalCol + "catVec"])
    cat_stages += [stringIndexer, encoder]

assemblerInputs = [c + "catVec" for c in categoricalColumns]
assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="featuresCat")
cat_stages += [assembler]

# create cat pipeline
pipelineCat = Pipeline(stages=cat_stages)

numericCols = ['homeLast5goals', 'awayLast5goals',
               'homeLast5shots_on', 'awayLast5shots_on']

assemblerNum = VectorAssembler(inputCols=numericCols, outputCol="featuresNum")

gbtc = GBTClassifier()

pipeline = Pipeline(stages=[pipelineCat, assemblerNum,
                            VectorAssembler(inputCols=["featuresCat",
                                                       "featuresNum"],
                                            outputCol="features"),
                            gbtc])

paramGrid = ParamGridBuilder() \
    .addGrid(gbtc.subsamplingRate, np.arange(0.1, 1.0, .1)) \
    .addGrid(gbtc.maxDepth, [1, 2, 3, 4, 5, 6, 8, 10]) \
    .build()

evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)

cvModel_gbtc = crossval.fit(trainData)

pred_gbtc = cvModel_gbtc.transform(testData)
score_gbtc = float(evaluator.evaluate(pred_gbtc,
                                      {evaluator.metricName: "areaUnderROC"}))
print(score_gbtc)

# need to use something like code below to save final model in S3 to be used by
# api

# import os
# import io

# obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
# s3_resource.Object('ddapi.data', 'bestPipe/').put(Body=cvModel_gbtc.bestModel.write().overwrite())

# type(cvModel_gbtc)

# spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
# spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SESCRET_KEY)
# spark._jsc.hadoopConfiguration().set("fs.s3n.endpoint", "us-east-2.amazonaws.com")
# spark._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
#spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3a.enableV4", "true")

#cvModel_lgreg.bestModel.write().overwrite().save("s3a://ddapi.data/bestPipetest")

#df = spark.read.csv("s3a://ddapi.data/modelDataFrame.csv")

#hadoop = spark._gateway.jvm.org.apache.hadoop
#import findspark
#findspark.init()

#df.write.parquet("s3a://ddapi.data/testdf.parquet", mode="overwrite")


#s3_bucket.put_object(cvModel_lgreg.bestModel.write().overwrite(), Key='bestPipe/')
# save best model to s3
#s3_bucket = s3_resource.Bucket('ddapi.data')

#for root, dirs, files in os.walk("bestPipe", topdown=False):
#   dd = '.DS_STORE'
   #files = [f for f in files if dd not in f]
#   for name in files:
#      if name != dd:
#      #if name[0] != '.':
#         repr(name)
#         print(name)
#         name
#         type(name)
#         print(os.path.join(root, name))
#         d = os.path.join(root, name)
#         s3_bucket.put_object(d, Key='bestPipe/')
#         s3_resource.Object('ddapi.data', 'bestPipe/').put(Body=d)


#      d = os.path.join(root, name)
#      s3_bucket.put_object(d, Key='bestPipe/')

   #for name in dirs:
    #  print(os.path.join(root, name))

#def pywalker(path):
#    for root, dirs, files in os.walk(path):
#        for dir in dirs:
#            for file_ in files:
            #    s3_bucket.upload_file(path+dir+'/'+file_, Key='bestPipe/')
#                print(path+dir+'/'+file_)
#pywalker('bestPipe/')


#    file_key_name = bestPipe + '/' + filename
#    local_path = os.getcwd()
#    local_name = local_path + '/' + key_name + filename
#    upload = s3_connect.upload_file(local_name, s3_bucket, file_key_name)

#if score_lgreg >= score_gbtc:
#    cvModel_lgreg.bestModel.write().overwrite().save('bestPipe')
#else:
#    cvModel_gbtc.bestModel.write().overwrite().save('bestPipe')
