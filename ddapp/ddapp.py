from flask import Flask
from flask_restplus import Api, Resource


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
