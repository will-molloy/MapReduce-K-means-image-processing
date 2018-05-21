package kmeans.service

import kmeans.model.PointColour
import kmeans.service.seeder.KMeansSeeder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec

class MapReduceKMeans(context: SparkContext, seeder: KMeansSeeder) extends SeqKMeans(seeder) {

  override def process(points: Array[PointColour], kClusters: Int): Seq[PointColour] = {
    iter.set(0)
    val centroids = seeder.seed(points, kClusters)
    val rddData = context.parallelize(points).cache()
    val result = iterate(rddData, centroids)
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
    log.info("Centroids changed (iterations: %d): %s.".format(iter.getAndIncrement(), centroids))
    val clusters = points
      .map(point => (point closest centroids) -> (point, 1))
      .reduceByKey {
        case ((pointA, countA), (pointB, countB)) => (pointA + pointB, countA + countB)
      }
      .map {
        case (centroid, (pointSum, countSum)) => centroid -> pointSum / countSum
      }
      .collectAsMap()

    // Extract new centroids
    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    if (converged(newCentroids, centroids)) newCentroids else iterate(points, newCentroids)
  }

}
