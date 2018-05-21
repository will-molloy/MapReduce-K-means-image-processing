package kmeans.service

import java.util.concurrent.atomic.AtomicLong

import kmeans.model.PointColour
import kmeans.service.seeder.KMeansSeeder
import org.apache.spark.internal.Logging

import scala.annotation.tailrec

class SeqKMeans(seeder: KMeansSeeder) extends Serializable with Logging {

  protected val iter = new AtomicLong(0)

  private val delta = 0.01

  /**
    * Given a list of points, find k centroids such that the squared distances between each point and their closest
    * centroid is minimised.
    *
    * Since finding the optimal solution is NP-Hard, a heuristic seeding is used then iterates until the means converge.
    */
  def process(points: Array[PointColour], kClusters: Int): Seq[PointColour] = {
    iter.set(0)
    val centroids = seeder.seed(points, kClusters)
    iterate(points, centroids)
  }

  def iterations(): Long = iter.get()

  protected final def converged(oldNew: (Seq[PointColour], Seq[PointColour])): Boolean = {
    !(oldNew.zipped exists { case (a, b) => (a dist b) > delta })
  }

  /**
    * Sequential KMeans implementation.
    */
  @tailrec
  private def iterate(points: Seq[PointColour], means: Seq[PointColour]): Seq[PointColour] = {
    log.info("Means changed (iterations: %d): %s.".format(iter.getAndIncrement(), means))
    // Repeat old means to preserve groupBy order
    val oldNew = points.groupBy(_ closest means).map { case (mean, cluster) => mean -> PointColour.average(cluster) }.toSeq.unzip
    if (converged(oldNew)) oldNew._2 else iterate(points, oldNew._2)
  }

}
