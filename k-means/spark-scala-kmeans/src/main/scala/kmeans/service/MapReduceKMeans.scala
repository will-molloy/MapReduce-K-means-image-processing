package kmeans.service

import kmeans.model.PointColour
import kmeans.service.KMeans.{converged, matchNewMeans}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

class MapReduceKMeans(context: SparkContext) extends KMeans {

  override def kMeans(points: Seq[PointColour], initialMeans: Seq[PointColour]): Seq[PointColour] = {
    val rddData = context.parallelize(points).cache()
    val result = iterate(rddData, initialMeans)
    rddData.unpersist()
    result
  }

  /**
    * MapReduce KMeans implementation.
    * Mapping stage: for each point, find closest centroid.
    * Intermediate key, [closest mean, (point , 1)].
    * Reduction stage: determine new centroids by averaging points assigned to current centroids.
    * I.e. pairwise sum of values then divide point sum by count sum.
    */
  @tailrec
  private def iterate(points: RDD[PointColour], centroids: Seq[PointColour]): Seq[PointColour] = {
    log.info(s"Centroids changed (${iter.getAndIncrement()} iterations)")
    val clusters = points
      .map(point => (point closest centroids) -> (point, 1))
      .reduceByKey { case ((pointA, countA), (pointB, countB)) => (pointA + pointB, countA + countB) }
      .collectAsMap
      .mapValues { case (pointSum, countSum) => pointSum / countSum }

    val newCentroids = matchNewMeans(centroids, clusters)
    if (converged(centroids, newCentroids)) newCentroids else iterate(points, newCentroids)
  }

}
