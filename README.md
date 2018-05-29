# "Big Data" processing with MapReduce framework
![](mapreduce.jpg)<sup>1</sup>      

## Research
This was a research project where we learnt the MapReduce programming model and investigated a few frameworks. We have two implemenations:

### K-means (main) implementation
* This processes images to determine the most common colour values.
* The source code for the k-means implementation is found under the [k-means directory](https://github.com/wilmol/MapReduce-K-means-image-processing/tree/master/k-means/spark-scala-kmeans).
* This includes instructions to run.

### reddit comment implementation
* This calculates the average comment score per sub reddit and was used to compare frameworks.
* The source code for the reddit comment implementations is found under the [reddit-comments directory](https://github.com/wilmol/MapReduce-K-means-image-processing/tree/master/reddit-comments).
* This has been grouped by framework (couchDB, Hadoop, Spark, Cloud Haskell).
* The sequential Java version is found within the Hadoop source code or [here](https://github.com/wilmol/MapReduce-K-means-image-processing/tree/master/reddit-comments/hadoop-reddit/src/main/java/nz/ac/auckland/mapreduce/NoFrameWorkMain.java).

## Runnables
* The latest binaries for all implementations are found zipped on the [releases page](https://github.com/wilmol/MapReduce-K-means-image-processing/releases).
* This includes input images/video (see the resources directory) and instructions to run so you can reproduce our results.
* The reddit comment data set from taken from [here](https://www.kaggle.com/reddit/reddit-comments-may-2015). We uncompressed it and took the first 20,000,000 lines (approx 11GB of JSON).

<sup>1</sup> Image credit: http://www.well-typed.com/blog/73/
