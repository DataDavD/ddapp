from flask import Flask, request
from flask_restplus import Api, Resource
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession


ddapp = Flask(__name__)
api = Api(app=ddapp,
          version='0.0.1',
          title='Total Goals Predictor',
          description='Flask API that serves PySpark LogReg model predictions')

lrmod = api.namespace('lrmodel',
                      description='API used to predict total goals of match')


@lrmod.route("/")
class LogReg(Resource):
    def get(self):
        """
        returns a prediction label and odds as JSON object
        """
        model_inputs = request.get_json

        df = spark.createDataFrame(model_inputs)
        lr_fit = bestPipe.transform(df)

        probs = lr_fit.select(lr_fit.probability)
        preds = lr_fit.select(lr_fit.prediction)

        return 'Hello Tester'


if __name__ == '__main__':
    ddapp.run(debug=True)

    spark = SparkSession \
        .builder \
        .appName("pysparkDDapp") \
        .getOrCreate()

    bestPipe = CrossValidator.load('bestLogmodel')
