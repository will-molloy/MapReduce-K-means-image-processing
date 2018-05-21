package kmeans.service.seeder

import kmeans.model.PointColour

import scala.util.Random

class KMeansPPSeeder extends KMeansSeeder {

  /**
    * Selects points with probability proportional to their distance from the current set of centroids.
    * This set is updated each iteration for a total of k iterations.
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("KMeans++ init, points %d.".format(data.length))
    val centroids = new Array[PointColour](k)
    centroids(0) = data(Random.nextInt(data.length))
    var costs = data.map(_ dist centroids(0))

    for (i <- 1 until k) {
      var weightedRand = costs.sum * Random.nextDouble()

      def sample(): Unit = {
        for (j <- data.indices) {
          weightedRand -= costs(j)
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

}
