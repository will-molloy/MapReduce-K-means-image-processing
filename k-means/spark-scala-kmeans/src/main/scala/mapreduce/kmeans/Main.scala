package mapreduce.kmeans

import java.io.File

import javax.imageio.ImageIO
import mapreduce.kmeans.service.{KMeansImageProcessor, MapReduceKMeans}
import org.apache.spark._

object Main {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: Main <pointsFile> <numClusters>")
      System.exit(-1)
    }
    val pointsFile = args(0)
    val kClusters = args(1) toInt

    val config = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Main")
    val context = new SparkContext(config)

    val image = ImageIO.read(new File(pointsFile))
    val processedImage = new KMeansImageProcessor(new MapReduceKMeans(context)).process(image, kClusters)

    ImageIO.write(processedImage, "png", new File("out.png"))
  }

}
