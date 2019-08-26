#!/usr/bin/env bash

# installs deps
sudo yum -y update
sudo python3 -m pip install --upgrade pip
sudo yum -y update
sudo python3 -m pip install awscli pandas requests pyarrow numpy pyspark
sudo yum -y update
