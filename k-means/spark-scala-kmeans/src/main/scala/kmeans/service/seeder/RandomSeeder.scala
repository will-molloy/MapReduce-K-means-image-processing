package kmeans.service.seeder

import kmeans.model.PointColour

class RandomSeeder extends KMeansSeeder {

  /**
    * Picks k points uniformly random from the data set
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("Random init, points %d.".format(data.length))
    Array.fill(k)(PointColour.uniformlyRandom(data))
  }

}
