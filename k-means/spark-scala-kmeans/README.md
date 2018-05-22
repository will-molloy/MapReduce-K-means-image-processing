## scala-spark-kmeans

usage: kmeans.Main pointsFile outFile numClusters repeats/frames [-h] [-p {mapreduce,imagesplit,sequential}] [-s {random,kmeans++,parallelkmeans++}]

KMeans image processing in parallel

positional arguments:
*  pointsFile
    * Input image/video file name  
*  outFile
    * Output file name  
*  numClusters
    * Number of clusters per image  
*  repeats/frames
    * Number of image repeats or max video frames  

named arguments:
*  -h, --help             show this help message and exit
* -p {mapreduce,imagesplit,sequential}, --parallel {mapreduce,imagesplit,sequential}  
    * Use MapReduce implementation (pixel partitioning) or Process images in parallel (image partitioning)
    * (default: sequential)
*  -s {random,kmeans++,parallelkmeans++}, --seeder {random,kmeans++,parallelkmeans++}  
    * KMeans seeder, KMeans++ produces a better result  
    * Will default to kmeans++ if parallelkmeans++ is specified with imagesplit
    * (default: random)