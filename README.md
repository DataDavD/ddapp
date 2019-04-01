# ddapp
Full stack data science project that builds machine learning model using Python and Spark (Spark SQL & MLlib) then implements into production via Flask API.  Eventually Airflow DAG on EC2 instance will be written that fetches new match data, spins up EMR cluster, runs PySpark script for data manipulation, model training, and saving the model so that the API can fetch the new predictions/odds, and finally, spinning down the EMR cluster. 

### current progress: 
- finished Spark SQL and python code to transform data and calculate new features
- finished logistic regression model using Spark MLlib 
- finished testing GBTClassifier (gradient boosted tree; didn't improve over logistic regression with similar features)
- saved logistic regression model
- created Flask API that takes in JSON of feature key/values and returns JSON of probabilites of each binary class and the predicted label

### next steps:
- finish writing EMR pyspark ETL script to pull new match data and retrain logistic regression model using Spark MLlib
- create python boto3 script to spin up EMR cluster, run ETL and model training script and then spin down EMR cluster
- automate ETL and model training using Airflow DAG
- deploy Airflow DAG on EC2 instance
- deploy Flask API using Zappa and AWS Lambda + API Gateway

### data source: 
https://datahub.io/sports-data/english-premier-league (includes field descriptions and csv/json data formats)

### help:
Please feel free to fork and submit pull requests.
