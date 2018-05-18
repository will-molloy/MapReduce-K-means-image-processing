import java.awt.Color
import java.awt.image.BufferedImage

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class KMeansMapReduceImageProcessor(val context: SparkContext, val kPoints: Int, val image: BufferedImage) {

  private val delta = 0.001

  def processImage(): BufferedImage = {
    val width = image.getWidth
    val height = image.getHeight

    // Persist into RDD
    val imageData = Array.tabulate[PointColourF](width * height)(i => {
      val pixel = image.getRGB(i / height, i % height)
      val r = (pixel >> 16) & 0xff
      val g = (pixel >> 8) & 0xff
      val b = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(r, g, b, arr)
      PointColourF(arr.apply(0), arr.apply(1), arr.apply(2))
    }
    )
    val rddData = context.parallelize(imageData).cache()

    // Process image via KMeans
    val centroids = initCentroids(imageData)
    kMeansIterate(rddData, centroids)

    // Update image
    for (a <- 0 until height * width) {
      val near = imageData.apply(a) ?? centroids
      near match {
        case PointColourF(h, s, b) =>
          val pixel = Color.HSBtoRGB(h.asInstanceOf[Float], s.asInstanceOf[Float], b.asInstanceOf[Float])
          image.setRGB(a / height, a % height, pixel)
      }
    }
    image
  }

  // KMeans++ implementation
  private def initCentroids(data: Array[PointColourF]): Array[PointColourF] = {
    val len = data.length
    val newCent = Array.tabulate(kPoints) { _ => PointColourF.origin() }
    val lengthBuffer = new Array[Double](len)
    for (n <- 1 until kPoints) {
      var sum = 0.0
      for (m <- 0 until len) {
        lengthBuffer(m) = (data(m) ?? newCent) >< data(m)
        sum += lengthBuffer(m)
      }
      sum *= math.random

      def f(): Unit = {
        for (m <- 0 until len) {
          sum -= lengthBuffer(m)
          if (sum < 0f) {
            newCent(n) = data(m)
            return
          }
        }
      }

      f()
    }
    newCent
  }

  // MapReduce KMeans implementation
  private def kMeansIterate(points: RDD[PointColourF], centroids: Seq[PointColourF]): Unit = {
    val clusters = points
      // Mapping stage, for each point, find closest centroid.
      // Intermediate key: [closest mean, (point , 1)].
      .map(point => (point ?? centroids) -> (point, 1))
      // Reduction stage, determine new centroids by averaging points assigned to current centroids.
      // I.e. pairwise sum of values then divide point sum by count sum.
      .reduceByKeyLocally {
      case ((pointA, countA), (pointB, countB)) => (pointA + pointB, countA + countB)
    }
      .map {
        case (centroid, (pointSum, countSum)) => centroid -> pointSum / countSum
      }

    // Extract new centroids
    val newCentroids = centroids.map(oldCentroid => {
      clusters.get(oldCentroid) match {
        case Some(newCentroid) => newCentroid
        case None => oldCentroid
      }
    })

    // Calculate the distance between the centroids old and new
    val delta = (centroids zip newCentroids).map { case (a, b) => a >< b }
    System.err.println("Centroids changed")

    // Determine if the centroids have converged, otherwise repeat
    if (delta.exists(_ > this.delta))
      kMeansIterate(points, newCentroids)
  }

}
