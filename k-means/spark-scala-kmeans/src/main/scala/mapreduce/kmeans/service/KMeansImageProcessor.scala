package mapreduce.kmeans.service

import java.awt.Color
import java.awt.image.BufferedImage

import mapreduce.kmeans.model.VectorPoint
import org.apache.spark.rdd.RDD

class KMeansImageProcessor(val kMeans: KMeans) {

  private val delta = 0.001

  def process(image: BufferedImage, kClusters: Int): BufferedImage = {
    val width = image getWidth
    val height = image getHeight

    // Read image to vector point array
    val imageData = (Array tabulate[VectorPoint] width * height)(i => {
      val pixel = image getRGB(i / height, i % height)
      val r = (pixel >> 16) & 0xff
      val g = (pixel >> 8) & 0xff
      val b = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(r, g, b, arr)
      VectorPoint(arr)
    })

    // Process image
    val resultCentroids = kMeans kMeans(imageData, kClusters, delta)

    // Update and return image
    for (a <- 0 until height * width) {
      val near = (imageData apply a) ?? resultCentroids
      near match {
        case VectorPoint(_, list) =>
          val newPixel = Color.HSBtoRGB(list(0) toFloat, list(1) toFloat, list(2) toFloat)
          image.setRGB(a / height, a % height, newPixel)
      }
    }
    image
  }

}
