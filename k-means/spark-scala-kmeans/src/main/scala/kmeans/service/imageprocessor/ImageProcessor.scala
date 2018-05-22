package kmeans.service.imageprocessor

import java.awt.Color
import java.awt.image.BufferedImage

import kmeans.model.PointColour
import kmeans.util.NumberFormatter
import org.apache.spark.internal.Logging

trait ImageProcessor extends Logging {

  /**
    * Processes images in place.
    * Returns total number of iterations.
    */
  def process(images: Seq[BufferedImage], kClusters: Int): Long

  def extractImageData(image: BufferedImage): Array[PointColour] = {
    val width = image getWidth
    val height = image getHeight

    val out = Array.tabulate[PointColour](width * height)(i => {
      if (i % 1000000 == 0) log.info("%s pixels read" format NumberFormatter(i))
      val pixel = image getRGB(i / height, i % height)
      val r = (pixel >> 16) & 0xff
      val g = (pixel >> 8) & 0xff
      val b = pixel & 0xff
      val arr = new Array[Float](3)
      Color.RGBtoHSB(r, g, b, arr)
      PointColour(arr(0), arr(1), arr(2))
    })
    log.info("Image read, %s pixels".format(NumberFormatter(width * height)))
    out
  }

  def updateImage(image: BufferedImage, centroids: Seq[PointColour], imagePoints: Array[PointColour]): Unit = {
    val width = image getWidth
    val height = image getHeight

    imagePoints.zipWithIndex.foreach { case (point, i) =>
      if (i % 1000000 == 0) log.info("%s pixels updated" format NumberFormatter(i))
      point closest centroids match {
        case PointColour(r, g, b) =>
          val newPixel = Color.HSBtoRGB(r, g, b)
          image.setRGB(i / height, i % height, newPixel)
      }
    }
    log.info("Image updated, %s pixels".format(NumberFormatter(width * height)))
  }

}