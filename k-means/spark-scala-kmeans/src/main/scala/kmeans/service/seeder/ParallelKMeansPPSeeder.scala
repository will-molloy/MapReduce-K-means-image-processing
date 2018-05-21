package kmeans.service.seeder

import kmeans.model.PointColour
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ParallelKMeansPPSeeder(val context: SparkContext) extends KMeansSeeder {

  /**
    * Parallel implementation of KMeans++.
    * Rather than select one point per iteration (total k iterations) a slightly different heuristic is used to
    * sample points independently and therefore select multiple points per iteration (usually total log(k) iterations).
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("Parallel KMeans++ init, points %d.".format(data.length))
    val seed = Random.nextInt()
    val rddData = context.parallelize(data).cache()
    val centroids = ArrayBuffer[PointColour]()
    var costs = rddData.map(_ => Double.PositiveInfinity)

    // Init, random point from the data set
    centroids ++= Array(rddData.takeSample(false, 1, seed).head)

    var i = 0
    while (centroids.length < k) {
      log.info("Iteration: %d." format i)

      // compute distances between each point and the centroids independently
      costs = rddData.zip(costs).map { case (point, cost) =>
        math.min(point distClosest centroids, cost)
      }.cache()

      // sample each point independently with probability proportional to their distance from the centroids
      val costSum = costs.sum()
      centroids ++= rddData.zip(costs).mapPartitionsWithIndex { (index, pointCosts) =>
        val rand = new Random(seed ^ (i << 16) ^ index)
        pointCosts.filter { case (_, cost) => rand.nextDouble() < 2.0 * cost * k / costSum }.map(_._1)
      }.collect()
      i += 1
    }
    costs.unpersist()
    log.info("Sampled in %d iterations." format i)

    val res = recluster(rddData, centroids.distinct, k)
    rddData.unpersist()
    res
  }

  // Recluster candidates into exactly k clusters
  private def recluster(rddData: RDD[PointColour], distinctCentroids: ArrayBuffer[PointColour], k: Int): Array[PointColour] = {
    if (distinctCentroids.length == k) {
      log.info("Exactly %d/%d centroids found." format(distinctCentroids.length, k))
      distinctCentroids.toArray
    } else if (distinctCentroids.length < k) {
      log.info("Only %d/%d centroids found, taking duplicates." format(distinctCentroids.length, k))
      Array.fill(math.ceil(k / distinctCentroids.length).toInt)(distinctCentroids).flatten.take(k)
    } else {
      log.info("Picking %d centroids from sample of %d." format(k, distinctCentroids.length))
      // Set weight to be the number of points mapping to each candidate centroid
      val weightedPoints = rddData.map(_ closest distinctCentroids).countByValue()
      // Continue via. sequential KMeans++ with significantly less data than the input
      new WeightedKMeansPPSeeder().seed(weightedPoints, k)
    }
  }

}
