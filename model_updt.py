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

csv_buffer = StringIO()
df.to_csv(csv_buffer)

obj = s3_resource.Object('ddapi.data', 'modelDataFrame.json')
s3_resource.Object('ddapi.data', 'bestPipe').put(Body=dd.save('bestPipe'))

type(cvModel)

cvModel.bestModel.write().overwrite().save(path="s3a://ddapi.data/bestPipe")
s3_bucket.put_object(cvModel.bestModel.write().overwrite(), Key='bestPipe/')
# save best model to s3
s3_bucket = s3_resource.Bucket('ddapi.data')

if score_lgreg <= score_gbtc:
    dd = cvModel.bestModel.write().overwrite().save('bestPipeLogReg')
    s3_bucket.put_object(Body=dd, Key='bestPipe')
else:
    s3_bucket.put_object(Body=json, Key='modelDataFrame.json')


s3_bucket.put_object(Body=json, Key='modelDataFrame.json')
