package kmeans.service.seeder

import kmeans.model.PointColour
import org.apache.spark.internal.Logging

import scala.collection.Map
import scala.util.Random

private[seeder] class WeightedKMeansPPSeeder extends Logging {

  /**
    * Selects points with probability proportional to their distance from the current set of centroids and the set of
    * given weights. This set is updated each iteration for a total of k iterations.
    *
    * Used as helper to finish another seeder.
    */
  def seed(weightedData: Map[PointColour, Long], k: Int): Array[PointColour] = {
    val data = weightedData.keys.toArray
    val weights = weightedData.values.toArray
    log.info("Weighted KMeans++ init, points %d.".format(data.length))
    val centroids = new Array[PointColour](k)
    centroids(0) = pickWeighted(data, weights)
    var costs = data.map(_ dist centroids(0))

    for (i <- 1 until k) {
      var weightedRand = costs
        .zip(weights)
        .map(costWeight => costWeight._1 * costWeight._2)
        .sum * Random.nextDouble()

      def sample(): Unit = {
        for (j <- data.indices) {
          weightedRand -= costs(j) * weights(j)
          if (weightedRand < 0) {
            centroids(i) = data(j)
            return
          }
        }
      }

      sample()

      // compute distances between each point and this iterations centroids
      costs = data.zip(costs).map { case (point, cost) =>
        math.min(point dist centroids(i), cost)
      }
    }
    centroids
  }

  private def pickWeighted(points: Array[PointColour], weights: Array[Long]): PointColour = {
    var weightedRand = weights.sum * Random.nextDouble()
    var i = 0
    while (i < points.length && weightedRand > 0) {
      weightedRand -= weights(i)
      i += 1
    }
    points(i - 1)
  }
}
