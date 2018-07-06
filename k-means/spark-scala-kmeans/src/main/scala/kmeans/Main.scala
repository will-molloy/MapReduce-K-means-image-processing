package kmeans

import java.io.{File, FileWriter}
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.Date

import javax.imageio.ImageIO
import kmeans.service.imageprocessor.{ParallelImageProcessor, SeqImageProcessor}
import kmeans.service.seeder.{KMeansPPSeeder, ParallelKMeansPPSeeder, RandomSeeder}
import kmeans.service.{MapReduceKMeans, SeqKMeans}
import kmeans.util._
import net.sourceforge.argparse4j.inf.ArgumentParserException
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

object Main extends Logging {

  val appName = "SE751-Group9-MapReduce-scala-spark-kmeans"
  val logFile: String = "%s.log" format appName
  val csvFile: String = "%s.csv" format appName

  def main(args: Array[String]) {
    val start = System.currentTimeMillis()

    // run the Spark job
    val config = new SparkConf()
      .setMaster("local[*]")
      .setAppName(appName)
    val context = new SparkContext(config)
    val fileWriter = new FileWriter(new File(logFile), true)
    val argParser = ArgumentParserFactory.get

    try {
      val ns = argParser.parseArgs(args)
      val pointsFile = ns.getString("pointsFile")
      val outFile = ns.getString("outFile")
      val kClusters = ns.getInt("numClusters").intValue()
      val repeats = ns.getInt("repeats/frames").intValue()
      val seeder = ns.getString("seeder") match {
        case "kmeans++" => new KMeansPPSeeder
        case "parallelkmeans++" =>
          ns.getString("parallel") match {
            case "imagesplit" => new KMeansPPSeeder
            case _ => new ParallelKMeansPPSeeder(context)
          }
        case _ => new RandomSeeder
      }
      val kMeans = ns.getString("parallel") match {
        case "mapreduce" => new MapReduceKMeans(context)
        case _ => new SeqKMeans
      }
      val imageProcessor = ns.getString("parallel") match {
        case "imagesplit" => new ParallelImageProcessor(context, seeder)
        case _ => new SeqImageProcessor(kMeans, seeder)
      }

      val contentType = Files.probeContentType(Paths.get(pointsFile))
      val images = contentType match {
        case a if a.contains("video") => VideoDecoder.decode(pointsFile, repeats)
        case a if a.contains("image") => List.fill(repeats)(ImageIO.read(new File(pointsFile)))
        case _ => throw new RuntimeException("Not an image or video file.")
      }

      // processing in place
      val totalIterations = imageProcessor.process(images, kClusters)

      contentType match {
        case a if a.contains("video") => GifEncoder.write(images, "%s.gif".format(outFile))
        case _ =>
          images.zipWithIndex.foreach { case (image, i) =>
            ImageIO.write(image, "png", new File("%s-%d.png".format(outFile, i)))
          }
      }

      val df = new SimpleDateFormat("yyyy/MM/dd HH-mm-ss")
      val date = new Date
      val logEntry = ("Completed: %s, " +
        "file: %s, " +
        "images: %d, clusters: %d, total iterations: %d, time(ms): %d, " +
        "image processor: %s, kmeans: %s, seeder: %s\n")
        .format(df.format(date), pointsFile, images.size, kClusters, totalIterations, System.currentTimeMillis() - start,
          imageProcessor.getClass.getSimpleName, kMeans.getClass.getSimpleName, seeder.getClass.getSimpleName)
      log.info(logEntry)
      fileWriter.write(logEntry)
    } catch {
      case e: ArgumentParserException => argParser.handleError(e)
        argParser.printHelp()
    } finally {
      fileWriter.flush()
      fileWriter.close()
    }
  }

}
