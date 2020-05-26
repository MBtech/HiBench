#!/bin/bash

aws s3 cp s3://house-pricing-data/base-data/data.csv s3://house-pricing-data/all-data/data.csv --recursive

python etl.py

python update.py

python ml.py