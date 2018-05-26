package kmeans.service.imageprocessor

import java.awt.image.BufferedImage

import kmeans.model.PointColour
import kmeans.service.SeqKMeans
import kmeans.service.seeder.{KMeansPPSeeder, RandomSeeder, Seeder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ParallelImageProcessor(context: SparkContext, seeder: Seeder) extends ImageProcessor {

  override def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    log.info("Starting, images: %d, clusters: %d.".format(images.size, kClusters))

    // Must extract locally, buffered image is not serialisable
    val imageData = images.map(extractImageData)

    // Must create kmeans and seeder within job otherwise serialisable error, hence new function for each implementation
    val rddData = context.parallelize(imageData).cache()
    val resultsIterations = seeder match {
      case a if a.isInstanceOf[RandomSeeder] => withRandomSeeder(rddData, kClusters)
      case a if a.isInstanceOf[KMeansPPSeeder] => withKmeansPPSeeder(rddData, kClusters)
      case _ => throw new RuntimeException("Invalid seeder for parallel image partitioning: " + seeder)
    }

    // Update locally
    (images zip resultsIterations._1 zip imageData) foreach (a => updateImage(a._1._1, a._1._2, a._2))
    resultsIterations._2.sum
  }

  def withRandomSeeder(rddData: RDD[Array[PointColour]], kClusters: Int): (Array[Seq[PointColour]], Array[Long]) = {
    rddData.map(new SeqKMeans().process(_, kClusters, new RandomSeeder)).collect().unzip
  }

  def withKmeansPPSeeder(rddData: RDD[Array[PointColour]], kClusters: Int): (Array[Seq[PointColour]], Array[Long]) = {
    rddData.map(new SeqKMeans().process(_, kClusters, new KMeansPPSeeder)).collect().unzip
  }
}
