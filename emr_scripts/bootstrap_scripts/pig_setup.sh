#!/bin/bash

sudo pip install findspark
sudo pip install pandas
sudo pip install numpy

sudo yum install -y tmux

sudo aws s3 sync s3://ds-emr-storage/step_scripts/pcgInputGeneration/ /home/hadoop/

#sudo chmod +x /usr/bin/test_upload/
#sudo chmod 0777 /usr/bin/test_upload/
