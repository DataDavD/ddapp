#!/usr/bin/env bash

echo "export PYSPARK_PYTHON=/usr/bin/python3" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_PYTHON_DRIVER=/usr/bin/python3" >> /etc/spark/conf/spark-env.sh
