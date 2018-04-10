import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
    val points = sc.textFile(pointsFile)
      .filter(line => !line.matches("^\\s*#.*"))
      .map(line => VectorPoint(line.split(",").drop(2).map(_.toDouble)))
      .cache

    val first = points.first
    System.err.print(first.toString)
    val centroids = Array.fill(k) { VectorPoint.random(first.len) }

    System.err.println("Read " + points.count() + " points.")

    // Start the Spark run
    val resultCentroids = kmeans(points, centroids, 0.1, sc)

    System.err.println("Final centroids: ")
    println(resultCentroids.map { _.toString } )
  }

  def kmeans(points: RDD[VectorPoint], centroids: Seq[VectorPoint], epsilon: Double, sc: SparkContext): Seq[VectorPoint] = {
    val clusters =
    points
      // Mapping stage, for each point, go thru each centroid and find the cloests
      .map(point => (point ?? centroids) -> (point, 1))
      // Reduce is done on a single thread
      .reduceByKeyLocally {
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
}

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

