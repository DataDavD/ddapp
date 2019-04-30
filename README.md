# ddapp
Full stack data science project that builds machine learning model using Python and Spark (Spark SQL & MLlib) then implements into production via Flask API.  Eventually Airflow DAG on EC2 instance will be written that fetches new match data, spins up EMR cluster, runs PySpark script for data manipulation, model training, and saving the model so that the API can fetch the new predictions/odds, and finally, spinning down the EMR cluster.

### current progress:
- finished Spark SQL and python code to transform data and calculate new features
- finished logistic regression model using Spark MLlib
- finished testing GBTClassifier (gradient boosted tree; didn't improve over logistic regression with similar features)
- saved logistic regression model
- created Flask API that takes in JSON of feature key/values and returns JSON of probabilites of each binary class and the predicted label
- finishing emr script using boto3 that spins up cluster, runs PySpark etl script, and spins down cluster: need to add in waiters

### next steps:
- refactor PySpark ETL script to use PySpark udfs instead of pandas where possible
- finish PySpark script that utilizes Spark MLlib and EMR cluster to save best pipeline/model object to s3 bucket
- automate EMR cluster creation, ETL, and model training scripts using Airflow DAG
- deploy Airflow DAG on EC2 instance
- connect Flask API to saved model in s3
- deploy Flask API using Zappa and AWS Lambda + API Gateway

### data source:
https://datahub.io/sports-data/english-premier-league (includes field descriptions and csv/json data formats)

### help:
Please feel free to fork and submit pull requests.  I would love any help about boto3 emr scripts :)
