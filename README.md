# Apache Beam samples

This project is composed by several samples. The purpose is to download and analyze GDELT Project data
using Apache Beam pipelines.

The objectives are:

1. Show how to implement Apache Beam pipelines for both streaming and batch analyses.
2. Show how to run those pipelines on several runners.

GDELT Project stores all news articles as "events": http://data.gdeltproject.org/events/index.html

Daily, a zip file is created, containing a CSV file with all events using the following format:

````
545037848       20150530        201505  2015    2015.4110                                                                                       JPN     TOKYO   JPN                                                             1       046     046     04      1       7.0     15      1       15      -1.06163552535792       0                                                       4       Tokyo, Tokyo, Japan     JA      JA40    35.685  139.751 -246227 4       Tokyo, Tokyo, Japan     JA      JA40    35.685  139.751 -246227 20160529        http://deadline.com/print-article/1201764227/
````

The format is described: http://data.gdeltproject.org/documentation/GDELT-Data_Format_Codebook.pdf

# Compiling and packaging the samples

It is simple, just use:

    mvn clean package

# Executing the examples

We have prepared maven profiles to execute the Pipelines in every single runner:

You must activate the profile and choose the appropiate runner:

Direct Runner

    mvn exec:java -Dexec.mainClass=org.apache.beam.samples.EventsByLocation -Pdirect-runner -Dexec.args="--runner=DirectRunner --input=/home/dataset/gdelt/2014-2016/201605*.zip --output=/tmp/gdelt/output/"

Spark Runner

    mvn exec:java -Dexec.mainClass=org.apache.beam.samples.EventsByLocation -Pspark-runner -Dexec.args="--runner=SparkRunner --input=/home/dataset/gdelt/2014-2016/201605*.zip --output=/tmp/gdelt/output/"

Flink Runner

    mvn exec:java -Dexec.mainClass=org.apache.beam.samples.EventsByLocation -Pflink-runner -Dexec.args="--runner=FlinkRunner --input=/home/dataset/gdelt/2014-2016/201605*.zip --output=/tmp/gdelt/output/"

Google Dataflow Runner

    mvn exec:java -Dexec.mainClass=org.apache.beam.samples.EventsByLocation -Pflink-runner -Dexec.args="--runner=DataflowRunner --input=/home/dataset/gdelt/2014-2016/201605*.zip --output=/tmp/gdelt/output/"

Google Dataflow Runner (blocking)

    mvn exec:java -Dexec.mainClass=org.apache.beam.samples.EventsByLocation -Pflink-runner -Dexec.args="--runner=BlockingDataflowRunner --input=/home/dataset/gdelt/2014-2016/201605*.zip --output=/tmp/gdelt/output/"

# Test infrastructure

Some of the samples require to have some infrastructure available, e.g. some
brokers, filesystems and databases.

We provide a convinient way to have such infrastructura available using docker-compose.

