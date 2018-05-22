package kmeans.service.imageprocessor

import java.awt.Color
import java.awt.image.BufferedImage

import kmeans.model.PointColour
import org.apache.spark.internal.Logging

trait ImageProcessor extends Logging {

  def process(images: Seq[BufferedImage], kClusters: Int): Long

  def extractImageData(image: BufferedImage): Array[PointColour] = {
    val width = image getWidth
    val height = image getHeight

    Array.tabulate[PointColour](width * height)(i => {
      val pixel = image getRGB(i / height, i % height)
      val r = (pixel >> 16) & 0xff
      val g = (pixel >> 8) & 0xff
      val b = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(r, g, b, arr)
      PointColour(arr(0), arr(1), arr(2))
    })
  }

  def updateImage(image: BufferedImage, centroids: Seq[PointColour], imagePoints: Array[PointColour]): Unit = {
    val height = image getHeight

    imagePoints.zipWithIndex.foreach { case (point, i) =>
      point closest centroids match {
        case PointColour(r, g, b) =>
          val newPixel = Color.HSBtoRGB(r, g, b)
          image.setRGB(i / height, i % height, newPixel)
      }
    }
  }

}