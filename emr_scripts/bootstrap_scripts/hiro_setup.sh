#!/bin/bash

sudo pip install -U \
	findspark \
	 pandas \
	 numpy \
	 awscli \
	 boto3

sudo yum install -y tmux

sudo aws s3 sync s3://ds-emr-storage/step_scripts/hiroTests/ /home/hadoop/hiro_tests/

sudo chmod +x /home/hadoop/hiro_tests/
sudo chmod 0777 /home/hadoop/hiro_tests/