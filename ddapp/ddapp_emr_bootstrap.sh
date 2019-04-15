#!/bin/bash

#sudo easy_install-3.6 pip
#sudo /usr/local/bin/pip3 install -r https://s3.us-east-2.amazonaws.com/ddapi.data/requirements.txt

# ----------------------------------------------------------------------
#              Install Anaconda (Python 3) & Set To Default
# ----------------------------------------------------------------------
wget https://repo.anaconda.com/archive/Anaconda3-2019.03-MacOSX-x86_64.sh -O ~/anaconda.sh
bash ~/anaconda.sh -b -p $HOME/anaconda
echo -e '\nexport PATH=$HOME/anaconda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc

# ----------------------------------------------------------------------
#                    Install Dependencies
# ----------------------------------------------------------------------
pip install -r https://s3.us-east-2.amazonaws.com/ddapi.data/requirements.txt

aws s3 cp s3://ddapi/emr_test.py /home/hadoop

# aws s3 cp s3://ddapi/requirements.txt /home/rq # add this, replace public s3 file
# pip install -r /home/rq/requirements.txt # add this instead of public s3 way

sudo yum -y update
