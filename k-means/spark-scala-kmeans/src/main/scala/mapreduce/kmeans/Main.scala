package mapreduce.kmeans

import java.io.File

import javax.imageio.ImageIO
import mapreduce.kmeans.service.KMeansMapReduceImageProcessor
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
    new KMeansMapReduceImageProcessor(context, kClusters, 0.001).process(image)

    ImageIO.write(image, "png", new File("out.png"))
  }

}

