package kmeans.service.seeder

import kmeans.model.PointColour
import kmeans.util.NumberFormatter

class RandomSeeder extends Seeder {

  /**
    * Picks k points uniformly random from the data set
    */
  override def seed(data: Array[PointColour], k: Int): Array[PointColour] = {
    log.info("Random init, %s points".format(NumberFormatter(data.length)))
    Array.fill(k)(PointColour.uniformlyRandom(data))
  }

}
