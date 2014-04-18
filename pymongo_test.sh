#!/bin/bash
export MONGOLAB_URI=mongodb://boczeratul:bocgg30cm@ds039349-a0.mongolab.com:39349/heroku_app23515786
echo "Starting interactive python with MongoLab connection established..."
python -i mongolab.py
