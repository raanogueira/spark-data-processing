#!/bin/bash

## Make sure you have a local version of spark (3.4+ required) up and running
./gradlew spark:shadowJar&&  spark-submit --class "spark.OddFinderRunner" --master local spark/build/libs/spark-1.0-all.jar spark/src/main/resources/input /tmp/spark/output apache.access.log

