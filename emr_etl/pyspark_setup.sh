#!/usr/bin/env bash

echo "export PYSPARK_PYTHON=python3" >> /etc/spark/conf/spark-env.sh
echo "export PYSPARK_PYTHON_DRIVER=python3" >> /etc/spark/conf/spark-env.sh
