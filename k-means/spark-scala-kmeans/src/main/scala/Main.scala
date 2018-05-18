import java.awt.Color
import java.io._
import java.util.concurrent.atomic.AtomicInteger

import javax.imageio.ImageIO
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object SparkKMeans {

  var atom : AtomicInteger = new AtomicInteger(0)
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkKMeans <pointsFile> <numClusters>")
      System.exit(-1)
    }

    val pointsFile = args(0)
    val k = args(1).toInt

    val sconf = new SparkConf().setAppName("SparkKMeans")
    val sc = new SparkContext(sconf)

    // Parse the points from a file into an RDD
    val img = ImageIO.read(new File(pointsFile))
    val w = img.getWidth
    val h = img.getHeight

    val imageData = Array.tabulate[PointColorF](w * h) (i => {
      val pixel = img.getRGB(i / h, i % h)
      val red = (pixel >> 16) & 0xff
      val green = (pixel >> 8) & 0xff
      val blue = pixel & 0xff
      val arr = new Array[Float](3)
      // Converts the RGB value to HSB value so it would better in our case
      Color.RGBtoHSB(red, green, blue, arr)
      PointColorF(arr.apply(0), arr.apply(1), arr.apply(2))
    }
    )

    val data = sc.parallelize(imageData).cache()

    val centroids = initCentroids(imageData, k)

    System.err.println("Read " + data.count() + " points.")

    // Start the Spark run
    val resultCentroids = kmeans(data, centroids, 0.001, sc)

    System.err.println("Final centroids: ")
    println(resultCentroids.map { _.toString } )

    for (a <- 0 until h*w) {
      val near = imageData.apply(a) ?? resultCentroids
      near match {
        case PointColorF(hh,ss,vv) => {
          val pixelColour = Color.HSBtoRGB(hh.asInstanceOf[Float],ss.asInstanceOf[Float], vv.asInstanceOf[Float])
          img.setRGB(a / h, a % h, pixelColour)
        }
      }
    }

    val outputFile = new File("out.png")
    ImageIO.write(img, "png", outputFile)

  }
  def kmeans(points: RDD[PointColorF], centroids: Seq[PointColorF], epsilon: Double = 0.001, sc: SparkContext): Seq[PointColorF] = {
    val clusters =
    points
      // Mapping stage, for each point, go thru each centroid and find the closest point
      .map(point => (point ?? centroids) -> (point, 1))
      // Reduce is done on a single thread
      .reduceByKeyLocally{
        case ((ptA, numA), (ptB, numB)) => (ptA + ptB, numA + numB)
      }
      // This is scala's map
      .map {
        case (centroid, (ptSum, numPts)) => centroid -> ptSum / numPts
      }

    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    // Calculate the new distance between the centroids old and new
    val delta = (centroids zip newCentroids).map{ case (a, b) => a >< b }

    //printClusters(newCentroids, points.collect())

    // If there is a delta above threshold
    if (delta.exists(_ > epsilon))
      kmeans(points, newCentroids, epsilon, sc)
    else
      return newCentroids
  }

  /**
    * Method to print the clusters for plotting
    * @param cents
    * @param pts
    */
  def printClusters(cents: Seq[PointColorF], pts: Seq[PointColorF]) = {
    val dict = new mutable.HashMap[PointColorF, ArrayBuffer[PointColorF]]()
    cents.foreach { e =>
      dict.put(e, new ArrayBuffer[PointColorF]())
    }
    pts.foreach { e =>
      val a = dict.get(e ?? cents)
      a.get += e
    }

    val f = new File("out" + atom.toString)
    val wr = new BufferedWriter(new FileWriter(f))

    def writeSeq(a : Seq[Any]) = {
      wr.write("[")
      for ((i,n) <- a.zip(0 to a.size)) {
        wr.write(i.toString)
        if (n != a.size - 1) {
          wr.write(",")
        }
        wr.flush()
      }
      wr.write("]")
    }

    wr.write("[")

    val dd = dict.toArray

    for (((k, v), n) <- dd.zip(0 to dd.length)) {
      wr.write(k.toString)
      wr.write(",")
      writeSeq(v)
      if (n != dd.length - 1) wr.write(",")
    }
    wr.write("]")

    wr.flush()
    atom.incrementAndGet()
    wr.close()

  }

  def initCentroids(data: Array[PointColorF], nPoints: Int): Array[PointColorF] = {
    // Kmeans++, we take a random point as starting point
    val sample = data.head
    val len = data.length
    val newCent = Array.tabulate(nPoints) { _ => PointColorF.origin() }
    val lengthBuffer = new Array[Double](len)
    for(n <- 1 until nPoints) {
      var sum = 0.0
      for(m <- 0 until len) {
        lengthBuffer(m) = (data(m) ?? newCent) >< data(m)
        sum += lengthBuffer(m)
      }
      sum *= math.random
      def f: Unit = {
        for (m <- 0 until len) {
          sum -= lengthBuffer(m)
          if (sum < 0f) {
            newCent(n) = data(m)
            return
          }
        }
      }
      f
    }
    println(newCent)
    newCent
  }
}

