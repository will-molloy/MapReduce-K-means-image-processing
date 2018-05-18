package mapreduce.kmeans.service

import java.awt.Color
import java.awt.image.BufferedImage

import mapreduce.kmeans.model.PointColour
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class KMeansMapReduceImageProcessor(val context: SparkContext, val kClusters: Int, val delta: Double) {

  def process(image: BufferedImage): BufferedImage = {
    val width = image getWidth
    val height = image getHeight

    // Read image data to HSB
    val imageData = (Array tabulate[PointColour] width * height)(i => {
      val pixel = image getRGB(i / height, i % height)
      val r = (pixel >> 16) & 0xff
      val g = (pixel >> 8) & 0xff
      val b = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(r, g, b, arr)
      PointColour(arr apply 0, arr apply 1, arr apply 2)
    })

    // Process image
    val resultCentroids = kMeans(imageData)

    // Update and return image
    for (a <- 0 until height * width) {
      val near = (imageData apply a) ?? resultCentroids
      near match {
        case PointColour(h, s, b) =>
          val newPixel = Color.HSBtoRGB(h.asInstanceOf[Float], s.asInstanceOf[Float], b.asInstanceOf[Float])
          image setRGB(a / height, a % height, newPixel)
      }
    }
    image
  }

  private def kMeans(points: Array[PointColour]): Seq[PointColour] = {
    val rddData = context.parallelize(points).cache()
    val seedCentroids = initCentroids(points)
    kMeansIterate(rddData, seedCentroids)
  }

  /**
    * KMeans++ implementation, seed the initial centroids.
    */
  private def initCentroids(data: Array[PointColour]): Array[PointColour] = {
    val len = data.length
    val newCentroids = Array.tabulate(kClusters) { _ => PointColour.origin() }
    val lengthBuffer = new Array[Double](len)
    for (n <- 1 until kClusters) {
      var sum = 0.0
      for (m <- 0 until len) {
        lengthBuffer(m) = (data(m) ?? newCentroids) >< data(m)
        sum += lengthBuffer(m)
      }
      sum *= math.random

      def f(): Unit = {
        for (m <- 0 until len) {
          sum -= lengthBuffer(m)
          if (sum < 0f) {
            newCentroids(n) = data(m)
            return
          }
        }
      }
      f()
    }
    newCentroids
  }

  /**
    * MapReduce KMeans implementation. Iterate given points until centroids converge.
    */
  private def kMeansIterate(points: RDD[PointColour], centroids: Seq[PointColour]): Seq[PointColour] = {
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
    System.err.println("Centroids changed.")

    // Determine if the centroids have converged, otherwise repeat
    if (delta.exists(_ > this.delta))
      kMeansIterate(points, newCentroids)
    else
      newCentroids
  }

}
