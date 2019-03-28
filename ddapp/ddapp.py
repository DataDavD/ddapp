from flask import Flask, request, jsonify
from flask_restplus import Api, Resource
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import PipelineModel
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


spark = SparkSession \
    .builder \
    .master('local') \
    .appName("pysparkDDapp") \
    .getOrCreate()

bestPipe = PipelineModel.load('bestPipeLogReg')

jsondd = [{"HomeTeam": "Burnley", "AwayTeam": "Huddersfield",
           "homeLast5goals": 5, "awayLast5goals": 2,
           "homeLast5shots_on": 10, "awayLast5shots_on": 10},
          {"HomeTeam": "Man City", "AwayTeam": "Wolves",
           "homeLast5goals": 10, "awayLast5goals": 10,
           "homeLast5shots_on": 30, "awayLast5shots_on": 25}]


@lrmod.route("/")
class LogReg(Resource):
    def post(self):
        """
        returns a prediction label and odds as JSON object
        """

        _model_inputs = request.get_json()

        #_df = spark.read.json(_model_inputs, multiLine=True)

        #lr_fit = bestPipe.transform(_df)

        #probs = lr_fit.select(lr_fit.probability)
        #preds = lr_fit.select(lr_fit.prediction)

        return jsonify(_model_inputs)


if __name__ == '__main__':
    ddapp.run(debug=True)
