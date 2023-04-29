#!/bin/bash

export CLUSTER_ID="testing"
export SPARK_JOB_S3_PATH="s3://sparktesting-app/emr-jar/app.jar"
export SPARK_JOB_CLASSPATH="spark.OddFinderRunner"
export SPARK_JOB_ARGUMENTS="s3a://sparktesting-app/input s3a://sparktesting-app/output"


