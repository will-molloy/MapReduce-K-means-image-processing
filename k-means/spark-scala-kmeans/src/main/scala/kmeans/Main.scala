package kmeans

import java.io.File
import java.nio.file.{Files, Paths}

import javax.imageio.ImageIO
import kmeans.service.seeder.{KMeansPPSeeder, ParallelKMeansPPSeeder, RandomSeeder}
import kmeans.service.{ImageProcessor, SeqKMeans, MapReduceKMeans}
import kmeans.util.{GifEncoder, VideoDecoder}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object Main extends Logging {

  val mapreduce_parallelism = true

  // kmeans++: slower but better result
  val kmeanspp = true
  val parallel_kmeanspp = true // priority over seq kmeans++

  def main(args: Array[String]) {
    val start = System.currentTimeMillis()
    if (args.length < 3) {
      usage()
    }
    val repeats = args(2) toInt
    val kClusters = args(1) toInt

    if (repeats < 1 || kClusters < 1) {
      usage()
    }

    val pointsFile = args(0)
    val contentType = Files.probeContentType(Paths.get(pointsFile))
    val images =
      if (contentType.contains("video")) {
        VideoDecoder.decode(pointsFile, repeats)
      } else if (contentType.contains("image")) {
        List.fill(repeats)(ImageIO.read(new File(pointsFile)))
      } else {
        null
      }
    if (images == null) {
      usage()
    }

    // run the Spark job
    val config = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Main")
    val context = new SparkContext(config)

    val seeder = if (parallel_kmeanspp) new ParallelKMeansPPSeeder(context) else if (kmeanspp) new KMeansPPSeeder else new RandomSeeder
    val kMeans = if (mapreduce_parallelism) new MapReduceKMeans(context, seeder) else new SeqKMeans(seeder)
    val totalIter = new ImageProcessor(kMeans).process(images, kClusters)

    if (contentType.contains("video")) {
      GifEncoder.write(images, "out.gif")
    } else {
      images.zipWithIndex.foreach { case (image, i) =>
        ImageIO.write(image, "png", new File("out-%d.png".format(i)))
      }
    }

    log.info("Complete, images: %d, clusters: %d, total iterations: %d, time: %dms, seeder: %s, kmeans: %s."
      .format(images.size, kClusters, totalIter, System.currentTimeMillis() - start, seeder.getClass.getSimpleName, kMeans.getClass.getSimpleName))
  }

  private def usage(): Unit = {
    System.err.println("Usage: kmeans.Main <image/video file> <numClusters> <repeats/frames>")
    System.exit(-1)
  }

}
