#!/bin/bash

## Make sure you have a local version of spark (3.4+ required) up and running
./gradlew oddFinderRunner build; spark-submit --class "spark.OddFinderRunner" --master local app/build/libs/oddFinderRunner.jar app/src/main/resources /tmp/spark/output apache.access.log


