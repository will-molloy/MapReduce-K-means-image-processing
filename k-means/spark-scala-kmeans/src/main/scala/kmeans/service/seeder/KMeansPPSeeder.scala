package kmeans.service.seeder

import kmeans.model.PointColour
import kmeans.service.seeder.KMeansPPSeeder.{sample, updateDistances}
import kmeans.util.NumberFormatter

import scala.util.Random

class KMeansPPSeeder extends Seeder {

  /**
    * Selects points with probability proportional to their distance from the current set of centroids.
    * This set is updated each iteration for a total of k iterations.
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("KMeans++ init, %s points".format(NumberFormatter(data.length)))
    val centroids = new Array[PointColour](k)
    centroids(0) = PointColour.uniformlyRandom(data)
    val distances = data.map(_ dist centroids(0))

    for (i <- 1 until k) {
      log.info("Iteration %d/%d" format(i + 1, k))
      centroids(i) = sample(data, distances)
      updateDistances(distances, data, centroids(i))
    }
    centroids
  }

}

object KMeansPPSeeder {

  def sample(points: Array[PointColour], costs: Array[Double]): PointColour = {
    var weightedRand = costs.sum * Random.nextDouble()
    var i = 0
    while (i < points.length && weightedRand > 0) {
      weightedRand -= costs(i)
      i += 1
    }
    points(i - 1)
  }

  def updateDistances(distances: Array[Double], points: Array[PointColour], centroid: PointColour): Unit = {
    for (j <- points.indices) {
      distances(j) = math.min(points(j) dist centroid, distances(j))
    }
  }

}