#!/usr/bin/env bash

# installs deps
sudo yum -y update
sudo python3 -m pip install --upgrade pip
sudo yum -y update
sudo python3 -m pip install awscli pandas requests
sudo yum -y update

# add the bash commands below if need to pip install from requirements.txt
# sudo aws s3 cp s3://ddapi.data/requirements.txt requirements.txt
# sudo python3 -m pip install -r requirements.txt
