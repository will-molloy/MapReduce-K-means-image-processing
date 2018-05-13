import java.awt.Color
import java.io.File
import java.util.stream.IntStream
import javax.imageio.ImageIO

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object SparkKMeans {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: SparkKMeans <pointsFile> <numClusters>")
      System.exit(-1)
    }

    val pointsFile = args(0)
    val k = args(1).toInt

    val sconf = new SparkConf().setAppName("SparkKMeans")
    val sc = new SparkContext(sconf)
    //val sc = SparkContext.getOrCreate()

    // Parse the points from a file into an RDD
    val img = ImageIO.read(new File(pointsFile))
    val w = img.getWidth
    val h = img.getHeight

    val imageData = Array.tabulate[PointColorF](w * h) (i => {
      val pixel = img.getRGB(i / h, i % h)
      val alpha = (pixel >> 24) & 0xff
      val red = (pixel >> 16) & 0xff
      val green = (pixel >> 8) & 0xff
      val blue = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(red, green, blue, arr)
      PointColorF(arr.apply(0), arr.apply(1), arr.apply(2))
      //PointColor(red, green, blue)
    }
    )

    val data = sc.parallelize(imageData).cache()

    val first = data.first
    System.err.print(first.toString)
    //val centroids = Array.fill(k) { PointColorF.random() }
    val centroids = initCentroids(imageData, k)

    System.err.println("Read " + data.count() + " points.")

    // Start the Spark run
    val resultCentroids = kmeans(data, centroids, 0.001, sc)

    System.err.println("Final centroids: ")
    println(resultCentroids.map { _.toString } )

    for (a <- 0 until h*w) {
      val near = imageData.apply(a) ?? centroids
      near match {
        case PointColorF(hh,ss,vv) => {
          val pixle = Color.HSBtoRGB(hh.asInstanceOf[Float],ss.asInstanceOf[Float], vv.asInstanceOf[Float])
          //val pixle = 0xff << 24 | r << 16 | g << 8 |  b
          img.setRGB(a / h, a % h, pixle)
        }
      }
    }

    val f = new File("out.png")
    ImageIO.write(img, "png", f)

  }

  def kmeans(points: RDD[PointColorF], centroids: Seq[PointColorF], epsilon: Double, sc: SparkContext): Seq[PointColorF] = {
    val clusters =
    points
      // Mapping stage, for each point, go thru each centroid and find the cloests
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

    System.err.println("Centroids changed")

    // If there is a delta above threshold
    if (delta.exists(_ > epsilon))
      kmeans(points, newCentroids, epsilon, sc)
    else
      return newCentroids
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
    newCent.toArray
  }
}

sealed case class PointColorF(r: Double, g: Double, b: Double) {
  def + (that : PointColorF) = PointColorF(r + that.r, g + that.g, b + that.b)
  def - (that : PointColorF) = PointColorF(r - that.r, g - that.g, b - that.b)
  def / (that : Double) = PointColorF((r / that) , (g / that), (b / that))
  def unary_! () = math.sqrt(r * r + b * b + g * g)
  def >< (that: PointColorF)  = !(this - that)
  def ?? (that: Seq[PointColorF]) = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)
  override def toString = s"$r $g $b"
}

object PointColorF {
  //def apply(r: Double, g: Double, b: Double): PointColorF = this(r, g, b)
  def random(): PointColorF = PointColorF(math.random, math.random, math.random)
  def origin(): PointColorF = PointColorF(0, 0, 0)
}

/*
sealed case class PointColor(r: Long, g: Long, b: Long) {
  def + (that : PointColor) = PointColor(r + that.r, g + that.g, b + that.b)
  def - (that : PointColor) = PointColor(r - that.r, g - that.g, b - that.b)
  def / (that : Double) = PointColor((r / that).round , (g / that).round, (b / that).round)
  def unary_! () = math.sqrt(r * r + b * b + g * g)
  def >< (that: PointColor)  = !(this - that)
  def ?? (that: Seq[PointColor]) = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)
  override def toString = s"$r $g $b"

}

object PointColor {
  def apply(r: Integer, g: Integer, b: Integer): PointColor = this(r, g, b)
  def random() : PointColor = PointColor((math.random * 255).asInstanceOf[Int], (math.random * 255).asInstanceOf[Int], (math.random * 255).asInstanceOf[Int])
}
*/

sealed case class VectorPoint(len: Integer, private val fromList: List[Double]) {

  def + (that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x + y })

  def - (that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x - y })

  def / (that: Double) = VectorPoint(this.fromList.map { x => x / that })

  def unary_- () = VectorPoint(this.fromList.map { x => -x })

  def unary_! () = math.sqrt(this.fromList.map { x => x * x }.sum)

  def >< (that: VectorPoint) : Double = !(this - that)

  def ?? (that: Seq[VectorPoint]) = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)

  override def toString = fromList.toString()
}

object VectorPoint {
  def apply(list: List[Double]) : VectorPoint = this(list.length, list)
  def apply(arr: Array[Double]) : VectorPoint = this(arr.length, arr.toList)
  def random(length: Integer) : VectorPoint = VectorPoint(Array.fill(length) { math.random })
}

