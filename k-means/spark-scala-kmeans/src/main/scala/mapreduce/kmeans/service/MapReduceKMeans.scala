package mapreduce.kmeans.service

import mapreduce.kmeans.model.VectorPoint
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * MapReduce KMeans implementation.
  */
class MapReduceKMeans(val context: SparkContext) extends KMeans {

  override def kMeans(points: Array[VectorPoint], kClusters: Int, delta: Double): Seq[VectorPoint] = {
    val rddData = context.parallelize(points).cache()
    val seedCentroids = initCentroids(points, kClusters)
    kMeansIterate(rddData, seedCentroids, delta)
  }

  /**
    * KMeans++ implementation, seed the initial centroids.
    */
  private def initCentroids(data: Array[VectorPoint], kClusters: Int): Array[VectorPoint] = {
    val len = data length
    val newCentroids = (Array tabulate kClusters) { _ => VectorPoint((Array fill len) (0.0)) } // init with origin
    val lengthBuffer = new Array[Double](len)

    for (n <- 1 until kClusters) {
      var sum = 0.0
      for (m <- 0 until len) {
        lengthBuffer(m) = (data(m) ?? newCentroids) >< data(m)
        sum += lengthBuffer(m)
      }
      sum *= math.random

      def f(): Unit = {
        for (m <- 0 until len) {
          sum -= lengthBuffer(m)
          if (sum < 0f) {
            newCentroids(n) = data(m)
            return
          }
        }
      }

      f()
    }
    newCentroids
  }

  /**
    * MapReduce KMeans implementation. Iterate given points until centroids converge.
    */
  private def kMeansIterate(points: RDD[VectorPoint], centroids: Seq[VectorPoint], delta: Double): Seq[VectorPoint] = {
    val clusters = points
      // Mapping stage, for each point, find closest centroid.
      // Intermediate key: [closest mean, (point , 1)].
      .map(point => (point ?? centroids) -> (point, 1))
      // Reduction stage, determine new centroids by averaging points assigned to current centroids.
      // I.e. pairwise sum of values then divide point sum by count sum.
      .reduceByKeyLocally {
      case ((pointA, countA), (pointB, countB)) => (pointA + pointB, countA + countB)
    }
      .map {
        case (centroid, (pointSum, countSum)) => centroid -> pointSum / countSum
      }

    // Extract new centroids
    val newCentroids = centroids map (oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    // Calculate the distance between the centroids old and new
    val currentDelta = (centroids zip newCentroids) map { case (a, b) => a >< b }
    System.err.println("Centroids changed.")

    // Determine if the centroids have converged, otherwise repeat
    if (currentDelta exists (_ > delta))
      kMeansIterate(points, newCentroids, delta)
    else
      newCentroids
  }

}
