package kmeans.service

import java.awt.Color
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicLong

import kmeans.model.PointColour
import org.apache.spark.internal.Logging

class ImageProcessor(val kMeans: SeqKMeans) extends Logging {

  private val count = new AtomicLong(0)

  private var totalIter = 0l

  def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    log.info("Starting, images: %d, clusters: %d.".format(images.size, kClusters))
    images.foreach(image => {
      val imageData = extractImageData(image)
      log.info("Read in image, %d points.".format(imageData.length))
      val resultCentroids = kMeans.process(imageData, kClusters)
      totalIter += kMeans.iterations()
      log.info("Final centroids (iterations: %d): %s.".format(kMeans.iterations(), resultCentroids))
      updateImage(image, resultCentroids, imageData)
    })
    totalIter
  }

  protected def extractImageData(image: BufferedImage): Array[PointColour] = {
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

  protected def updateImage(image: BufferedImage, centroids: Seq[PointColour], imagePoints: Array[PointColour]): BufferedImage = {
    val height = image getHeight

    imagePoints.zipWithIndex.foreach { case (point, i) =>
      point closest centroids match {
        case PointColour(r, g, b) =>
          val newPixel = Color.HSBtoRGB(r, g, b)
          image.setRGB(i / height, i % height, newPixel)
      }
    }
    log.info("Images processed: %d.".format(count.incrementAndGet()))
    image
  }

}
