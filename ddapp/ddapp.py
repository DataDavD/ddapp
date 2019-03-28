from flask import Flask, request, jsonify
from flask_restplus import Api, Resource
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

import pandas as pd


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


@lrmod.route("/")
class LogReg(Resource):
    def post(self):
        """
        returns a prediction label and odds as JSON object
        """

        _model_inputs = request.get_json()

        #_df = spark.read.json(_model_inputs)

        df0 = pd.read_json('dataTest.json', orient='columns')
        _df = spark.createDataFrame(df0)

        lr_fit = bestPipe.transform(_df)

        split1_udf = udf(lambda value: value[0].item(), FloatType())
        split2_udf = udf(lambda value: value[1].item(), FloatType())

        output = lr_fit.select(split1_udf('probability').alias('neg_prob'),
                               split2_udf('probability').alias('pos_prob'),
                               lr_fit.prediction)

        df1 = output.toPandas()
        df2 = df1.to_json(orient='records')

        return jsonify(df2)


if __name__ == '__main__':
    ddapp.run(debug=True)
