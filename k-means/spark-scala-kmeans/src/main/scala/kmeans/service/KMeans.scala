package kmeans.service

import java.util.concurrent.atomic.AtomicLong

import kmeans.model.PointColour
import kmeans.service.KMeans.incrementAndGetImageCount
import kmeans.service.seeder.Seeder
import org.apache.spark.internal.Logging

import scala.collection.Map

trait KMeans extends Logging {

  protected val iter = new AtomicLong(0)

  final def process(points: Array[PointColour], kClusters: Int, seeder: Seeder): (Seq[PointColour], Long) = {
    require(kClusters < points.length, "More points than k.")
    iter.set(0)
    val centroids = seeder.seed(points, kClusters)
    val result = kMeans(points, centroids)
    log.info("Image %d processed (%d iterations)".format(incrementAndGetImageCount, iter.get()))
    require(result.length == kClusters, "Set of means is not of size k.")
    (result, iter.get())
  }

  /**
    * Given a list of points, find k centroids such that the squared distances between each point and their closest
    * centroid is minimised.
    *
    * Since finding the optimal solution is NP-Hard, a heuristic seeding is used then iterates until the means converge.
    */
  protected def kMeans(points: Seq[PointColour], initialMeans: Seq[PointColour]): Seq[PointColour]

}

object KMeans {

  private val delta = 0.01

  private val imageCount = new AtomicLong(0)

  def incrementAndGetImageCount: Long = imageCount.incrementAndGet()

  def matchNewMeans(oldMeans: Seq[PointColour], clusters: Map[PointColour, PointColour]): Seq[PointColour] = {
    oldMeans.map(oldMean => {
      clusters.get(oldMean) match {
        case Some(newMean) => newMean
        case None => oldMean
      }
    })
  }

  def converged(oldMeans: Seq[PointColour], newMeans: Seq[PointColour]): Boolean = {
    require(oldMeans.length == newMeans.length, "New means length doesn't match old means.")
    !(oldMeans zip newMeans exists { case (a, b) => (a dist b) > delta })
  }

}