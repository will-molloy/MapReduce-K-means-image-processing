package kmeans.service

import kmeans.model.PointColour
import kmeans.service.KMeans.{converged, matchNewMeans}

import scala.annotation.tailrec

class SeqKMeans extends KMeans {

  override def kMeans(points: Seq[PointColour], initialMeans: Seq[PointColour]): Seq[PointColour] = iterate(points, initialMeans)

  /**
    * Sequential KMeans implementation.
    */
  @tailrec
  private def iterate(points: Seq[PointColour], means: Seq[PointColour]): Seq[PointColour] = {
    log.info("Means changed (%d iterations)".format(iter.getAndIncrement()))
    val clusters = points.groupBy(_ closest means).mapValues(PointColour.average)
    val newMeans = matchNewMeans(means, clusters)
    if (converged(means, newMeans)) newMeans else iterate(points, newMeans)
  }

}
