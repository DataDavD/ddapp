from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np

sc = SparkContext(appName="ddapp_test")

spark = SparkSession \
    .builder \
    .appName("DDapp_model_updt") \
    .getOrCreate()

df = spark.read.json('s3://ddapi.data/modelDataFrame.json')
# df = spark.read.load('s3://ddapi.data/modelDataFrame.csv', format='csv')

# split data first
trainData, testData = df.randomSplit([0.75, 0.25], seed=12345)

# train and get model score (ROC AUC) using logistic regression
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
    .addGrid(lr.regParam, np.arange(0, 1, .2)) \
    .addGrid(lr.elasticNetParam, np.arange(0, 1, .2)) \
    .build()

evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')

crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3)

cvModel_lgrg = crossval.fit(trainData)

pred_lgreg = cvModel_lgrg.transform(testData)
score_lgreg = float(evaluator.evaluate(pred_lgreg,
                                       {evaluator.metricName: "areaUnderROC"}))

# train and get model score (ROC AUC) using gradient boosted tree
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
    .addGrid(gbtc.maxDepth, [1, 3, 5]) \
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

if score_lgreg >= score_gbtc:
    cvModel_lgrg.bestModel.write().overwrite().save('s3://ddapi.data/bestPipe')
else:
    cvModel_gbtc.bestModel.write().overwrite().save('s3://ddapi.data/bestPipe')
