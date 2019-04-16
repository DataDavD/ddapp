#!/usr/bin/env bash

#sudo easy_install-3.6 pip
#sudo /usr/local/bin/pip3 install -r https://s3.us-east-2.amazonaws.com/ddapi.data/requirements.txt


# install conda
wget https://repo.anaconda.com/archive/Anaconda3-4.2.0-Linux-x86_64.sh-O ~/anaconda.sh
bash ~/anaconda.sh -b -p $HOME/anaconda
echo -e '\nexport PATH=$HOME/anaconda/bin:$PATH' >> $HOME/.bashrc && source $HOME/.bashrc

# installs deps
pip install -r https://s3.us-east-2.amazonaws.com/ddapi.data/requirements.txt
pip install yaml
pip install aws-cli

# aws s3 cp s3://ddapi/requirements.txt /home/rq # add this, replace public s3 file
# pip install -r /home/rq/requirements.txt # add this instead of public s3 way

sudo yum -y update
