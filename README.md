# Update
I just started as a data engineer @ Atlassian and have been focused on working on getting commits on Bitbucket :) recently.  I will get back to finishing this project as soon as I get settled into my role and projects at work.  If anyone wants to clone, dev, and submit a PR I would be really happy!

# ddapp
Full stack data science project that builds machine learning model using Python and Spark (Spark SQL & MLlib) then implements into production via Flask API.  Eventually Airflow DAG on EC2 instance will be written that fetches new match data, spins up EMR cluster, runs PySpark script for data manipulation, model training, and saving the model so that the API can fetch the new predictions/odds, and finally, spinning down the EMR cluster.

### current progress
- finished Spark SQL and python code to transform data and calculate new features
- finished logistic regression model using Spark MLlib
- finished testing GBTClassifier (gradient boosted tree; didn't improve over logistic regression with similar features)
- saved logistic regression model
- created Flask API that takes in JSON of feature key/values and returns JSON of probabilities of each binary class and the predicted label
- converted Jupyter Notebook code to Python and PySpark/Pandas ETL script that saves final Spark dataframe as csv to S3
- finished PySpark/Pandas script that utilizes Spark MLlib and EMR cluster to train two models (logistic regression and gradient boosted tree) and save the best pipeline/model object to s3 bucket
- finished writing Python/boto3 script that configures and spins up AWS EMR cluster, runs PySpark ETL script and model training script, and after spins down cluster (with waiters)
- finished skeleton Airflow DAG

### next steps
- refactor PySpark ETL script to use PySpark udfs instead of Pandas where possible
- automate EMR cluster creation, ETL, and model training scripts using Airflow DAG
- deploy Airflow DAG on EC2 instance
- connect Flask API to saved model in s3
- deploy Flask API using Zappa and AWS Lambda + API Gateway

### smaller things that should be fixed/changed/added
- add VPC to EMR cluster creation
- add logging
- add code documentation

### data source:
https://datahub.io/sports-data/english-premier-league (includes field descriptions and csv/json data formats)

### help:
Please feel free to fork and submit pull requests.  I would love any help about boto3 emr scripts :)
