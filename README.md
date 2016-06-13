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
