package kmeans.service.seeder

import kmeans.model.PointColour
import org.apache.spark.internal.Logging

trait KMeansSeeder extends Logging {

  def seed(data: Array[PointColour], kClusters: Int): Array[PointColour]

}
