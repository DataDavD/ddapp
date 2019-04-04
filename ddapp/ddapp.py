from flask import Flask, request, jsonify
from flask_restplus import Api, Resource
from pyspark.ml.feature import OneHotEncoderEstimator, VectorAssembler
from pyspark.ml.feature import StringIndexer, StandardScaler
from pyspark.ml import PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType


spark = SparkSession \
    .builder \
    .master('local') \
    .appName("pysparkDDapp") \
    .getOrCreate()

bestPipe = PipelineModel.load('bestPipeLogReg')


ddapp = Flask(__name__)
api = Api(app=ddapp,
          version='0.0.1',
          title='Total Goals Predictor',
          description='Flask API that serves PySpark LogReg model predictions')

lrmod = api.namespace('lrmodel',
                      description='API used to predict total goals of match')


@lrmod.route("/")
class LogReg(Resource):
    def post(self):
        """
        returns a prediction label and odds as JSON object
        """

        _model_inputs = request.get_json()

        _df = spark.createDataFrame(Row(**x) for x in _model_inputs)

        _lr_fit = bestPipe.transform(_df)

        split1_udf = udf(lambda value: value[0].item(), FloatType())
        split2_udf = udf(lambda value: value[1].item(), FloatType())

        output = _lr_fit.select(split1_udf('probability').alias('neg_prob'),
                                split2_udf('probability').alias('pos_prob'),
                                _lr_fit.prediction)

        # need to change line below to go directly from spark DF to JSON
        df = (output.toPandas()).to_json(orient='records')

        return jsonify(df)


if __name__ == '__main__':
    ddapp.run(debug=True)
