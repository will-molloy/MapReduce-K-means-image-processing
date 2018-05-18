package mapreduce.kmeans

import java.io.File
import java.nio.file.{Files, Paths}

import javax.imageio.ImageIO
import mapreduce.kmeans.service.{KMeansImageProcessor, MapReduceKMeans}
import mapreduce.kmeans.util.{GifEncoder, VideoDecoder}
import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]) {
    if (args.length != 2) {
      usage()
    }
    val kClusters: Int = args(1) toInt
    val pointsFile = args(0)
    val contentType = Files.probeContentType(Paths.get(pointsFile))

    val images = if (contentType.contains("video")) VideoDecoder.toImageList(pointsFile)
    else if (contentType.contains("image")) ImageIO.read(new File(pointsFile)) :: Nil
    else null
    if (images == null || kClusters < 0) {
      usage()
    }

    val config = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Main")
    val context = new SparkContext(config)

    // run the Spark job
    val processedImages = new KMeansImageProcessor(new MapReduceKMeans(context)).process(images, kClusters)

    if (processedImages.size > 1) {
      GifEncoder.write(processedImages, "out.gif")
    } else {
      ImageIO.write(processedImages(0), "png", new File("out.png"))
    }
  }

  private def usage(): Unit = {
    System.err.println("Usage: Main <image/video file> <numClusters>")
    System.exit(-1)
  }

}
