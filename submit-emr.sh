#!/bin/bash

export CLUSTER_ID="testing"
export SPARK_JOB_S3_PATH="s3://sparktesting-app/spark-jobs/spark-1.0-all.jar" #change it to where your s3 jar is located
export SPARK_JOB_CLASSPATH="spark.OddFinderRunner"
export SPARK_JOB_ARGUMENTS="s3a://sparktesting-app/input s3a://sparktesting-app/output"

./gradlew emr:run
