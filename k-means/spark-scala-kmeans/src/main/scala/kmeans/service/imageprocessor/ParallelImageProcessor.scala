package kmeans.service.imageprocessor

import java.awt.image.BufferedImage

import kmeans.model.PointColour
import kmeans.service.SeqKMeans
import kmeans.service.seeder.{KMeansPPSeeder, KMeansSeeder, RandomSeeder}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class ParallelImageProcessor(context: SparkContext, seeder: KMeansSeeder) extends ImageProcessor {

  override def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    log.info("Starting, images: %d, clusters: %d.".format(images.size, kClusters))

    // Must extract locally, buffered image is not serialisable
    val imageData = images.map(extractImageData)
    log.info("Read in images, %d points each.".format(imageData(0).length))

    // Must create kmeans and seeder within job otherwise serialisable error, hence new function for each implementation
    val rddData = context.parallelize(imageData).persist(StorageLevel.MEMORY_AND_DISK)
    val resultsIterations =
      (if (seeder.isInstanceOf[RandomSeeder]) withRandomSeeder(rddData, kClusters) else withKmeansPPSeeder(rddData, kClusters))
        .collect().unzip

    // Update locally
    (images zip resultsIterations._1 zip imageData) foreach (a => updateImage(a._1._1, a._1._2, a._2))
    resultsIterations._2.sum
  }

  def withRandomSeeder(rddData: RDD[Array[PointColour]], kClusters: Int): RDD[(Seq[PointColour], Long)] = {
    rddData.map(new SeqKMeans(new RandomSeeder).process(_, kClusters))
  }

  def withKmeansPPSeeder(rddData: RDD[Array[PointColour]], kClusters: Int): RDD[(Seq[PointColour], Long)] = {
    rddData.map(new SeqKMeans(new KMeansPPSeeder).process(_, kClusters))
  }
}
