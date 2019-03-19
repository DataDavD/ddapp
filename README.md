# ddapp
Full stack data science project that builds machine learning model using Python and Spark Spark SQL & MLlib) then implements into production via Flask app/API.  Eventually Airflow DAG on EC2 instance will be rewritten that fetches new match data, spins up EMR cluster, runs PySpark script for data manipulation, model training, and saving the model so that the API can fetch the new predictions/odds, and finally, closing down the EMR cluster. 

### current progress: 
- finished Spark SQL and python code to transform data and calculate new features
- finished logistic regression model using Spark MLlib 
- next, will finish testing GBTClassifier (gradient boosted tree) and then implement json predictions/odds as flask API/app

### data source: 
https://datahub.io/sports-data/english-premier-league (includes field descriptions and csv/json data formats)
