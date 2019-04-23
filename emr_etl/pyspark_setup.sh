#!/usr/bin/env bash

# bind conda to spark
#echo -e "\nexport PYSPARK_PYTHON=/home/hadoop/conda/bin/python" >> /etc/spark/conf/spark-env.sh
#echo "export PYSPARK_DRIVER_PYTHON=/home/hadoop/conda/bin/python" >> /etc/spark/conf/spark-env.sh

echo "export PYSPARK_PYTHON=python3" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_PYTHON_DRIVER=python3" >> /etc/spark/conf/spark-env.sh
