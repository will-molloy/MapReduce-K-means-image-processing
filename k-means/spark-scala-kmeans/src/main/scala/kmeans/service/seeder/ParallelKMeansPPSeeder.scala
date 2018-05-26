package kmeans.service.seeder

import kmeans.model.PointColour
import kmeans.service.seeder.KMeansPPSeeder.{sample, updateDistances}
import kmeans.util.NumberFormatter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class ParallelKMeansPPSeeder(val context: SparkContext) extends Seeder {

  /**
    * Parallel implementation of KMeans++.
    * Rather than select one point per iteration (total k iterations) a slightly different heuristic is used to
    * sample points independently and therefore select multiple points per iteration (usually total log(k) iterations).
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("Parallel KMeans++ init, %s points".format(NumberFormatter(data.length)))
    val seed = Random.nextInt()
    val rddData = context.parallelize(data).cache()
    val centroids = ArrayBuffer[PointColour]()
    var costs = rddData.map(_ => Double.PositiveInfinity)

    // Init, random point from the data set
    centroids ++= Array(rddData.takeSample(withReplacement = false, 1, seed).head)

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
    distinctCentroids.length match {
      case a if a == k =>
        log.info("Exactly %d/%d centroids found." format(distinctCentroids.length, k))
        distinctCentroids.toArray
      case a if a < k =>
        log.info("Only %d/%d centroids found, taking duplicates." format(distinctCentroids.length, k))
        Array.fill(math.ceil(k / distinctCentroids.length).toInt)(distinctCentroids).flatten.take(k)
      case _ =>
        log.info("Picking %d centroids from sample of %d." format(k, distinctCentroids.length))
        // Set weight to be the number of points mapping to each candidate centroid
        val weightedPoints = rddData.map(_ closest distinctCentroids).countByValue()
        // Continue via. sequential weighted KMeans++ with significantly less data than the input
        seedWeighted(weightedPoints.toArray.unzip, k)
    }
  }

  /**
    * Selects points with probability proportional to their distance from the current set of centroids and the set of
    * given weights. This set is updated each iteration for a total of k iterations.
    */
  private def seedWeighted(weightedData: (Array[PointColour], Array[Long]), k: Int): Array[PointColour] = {
    log.info("Weighted KMeans++ init, %s points".format(NumberFormatter(weightedData._1.length)))
    val data = weightedData._1
    val weights = weightedData._2.map(_.doubleValue())
    val centroids = new Array[PointColour](k)
    centroids(0) = sample(data, weights)
    val distances = data.map(_ dist centroids(0))

    for (i <- 1 until k) {
      log.info("Iteration %d/%d" format(i + 1, k))
      centroids(i) = sample(data, distances.zip(weights).map(distWeight => distWeight._1 * distWeight._2))
      updateDistances(distances, data, centroids(i))
    }
    centroids
  }

}
