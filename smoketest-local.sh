#!/bin/bash

./gradlew spark:run --args="src/main/resources/input /tmp/spark/output"
