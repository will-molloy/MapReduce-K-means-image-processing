package kmeans.service.imageprocessor

import java.awt.image.BufferedImage

import kmeans.service.KMeans

class SeqImageProcessor(kMeans: KMeans) extends ImageProcessor {

  override def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    var totalIter = 0l
    log.info("Starting, images: %d, clusters: %d.".format(images.size, kClusters))
    images.foreach(image => {
      val imageData = extractImageData(image)
      val resultsIterations = kMeans.process(imageData, kClusters)
      totalIter += resultsIterations._2
      updateImage(image, resultsIterations._1, imageData)
    })
    totalIter
  }

}
