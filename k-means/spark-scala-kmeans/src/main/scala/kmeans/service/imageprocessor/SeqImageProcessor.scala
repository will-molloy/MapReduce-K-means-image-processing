package kmeans.service.imageprocessor

import java.awt.image.BufferedImage

import kmeans.service.SeqKMeans

class SeqImageProcessor(kMeans: SeqKMeans) extends ImageProcessor {

  override def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    var totalIter = 0l
    log.info("Starting, images: %d, clusters: %d.".format(images.size, kClusters))
    images.foreach(image => {
      val imageData = extractImageData(image)
      log.info("Read in image, %d points.".format(imageData.length))
      val resultsIterations = kMeans.process(imageData, kClusters)
      val resultCentroids = resultsIterations._1
      totalIter += resultsIterations._2
      updateImage(image, resultCentroids, imageData)
    })
    totalIter
  }

}
