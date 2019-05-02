#!/usr/bin/env bash

# installs deps
# sudo pip3 install -r https://s3.us-east-2.amazonaws.com/ddapi.data/requirements.txt
sudo aws s3 cp s3://ddapi/requirements.txt /home/rq # add this, replace public s3 file
sudo pip3 install -r /home/rq/requirements.txt

sudo yum -y update
