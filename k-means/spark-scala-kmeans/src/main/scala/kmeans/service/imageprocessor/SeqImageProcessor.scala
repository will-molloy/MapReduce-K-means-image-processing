package kmeans.service.imageprocessor

import java.awt.image.BufferedImage

import kmeans.service.KMeans

class SeqImageProcessor(kMeans: KMeans) extends ImageProcessor {

  override def process(images: Seq[BufferedImage], kClusters: Int): Long = {
    var totalIter = 0l
    log.info(s"Starting, images: ${images.size}, clusters: $kClusters.")
    images.foreach(image => {
      val imageData = extractImageData(image)
      val resultsIterations = kMeans.process(imageData, kClusters)
      totalIter += resultsIterations._2
      updateImage(image, resultsIterations._1, imageData)
    })
    totalIter
  }

}
