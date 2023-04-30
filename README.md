A simple spark for processing CSV and TSV datasets in order to find out which values for a respective key occurred an odd number of times.

A concrete examle:

input1.csv:

```
key,value
1,2
1,2
1,2
1,5
1,5
3,4
```

output.tsv:

```
key,value
1,2
3,4
```

As noticed above, the number 2 occurred was the only value for the key 1 that occurred an odd number of times. The same applies to the key 3 and value 4

The spark job has support to reading and writing from/to S3, a simple app that allows you to submit your job to Amazon Elastic Map Reduce (EMR) 

# Project Structure

```
├── README.md
├── emr
│   ├── build.gradle.kts
│   └── src
├── spark
│   ├── build.gradle.kts
│   └── src
├── gradlew
├── settings.gradle.kts
├── build.gradle.kts
├── smoketest-local.sh
├── smoketest-s3.sh
├── submit-emr.sh
└── submit-local.sh

```


# Getting Started

1 - Run the spark job locally using the sample CSV and TSV inputs:

```
./smoketest-local.sh
```

2 - Run the spark job locally reading and writing to S3:

```
./smoketest-s3.sh
```

3 - Build the fat jar for the spark job and all its dependencies and then submit it to your local spark cluster:

```
./submit-local.sh
```

4 - Build the fat jar for the spark job and submit it to a EMR cluster:

```
./submit-emr.sh
```

# Testing 

The project contains the following tests:

1 - Unit tests to validate base cases
2 - Property-based tests to ensure that all requirements are met regardless of the inputs
3 - Integration tests to validate that the spark job is able to read and write to s3

To run all tests:

```
./gradlew test
```