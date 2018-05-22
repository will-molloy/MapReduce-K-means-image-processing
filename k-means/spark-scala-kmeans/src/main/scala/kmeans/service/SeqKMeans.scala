package kmeans.service

import java.util.concurrent.atomic.AtomicLong

import kmeans.model.PointColour
import kmeans.service.seeder.KMeansSeeder
import org.apache.spark.internal.Logging

import scala.annotation.tailrec

class SeqKMeans(seeder: KMeansSeeder) extends Logging {

  protected val iter = new AtomicLong(0)

  private val delta = 0.01

  /**
    * Given a list of points, find k centroids such that the squared distances between each point and their closest
    * centroid is minimised.
    *
    * Since finding the optimal solution is NP-Hard, a heuristic seeding is used then iterates until the means converge.
    */
  def process(points: Array[PointColour], kClusters: Int): (Seq[PointColour], Long) = {
    iter.set(0)
    val centroids = seeder.seed(points, kClusters)
    val result = iterate(points, centroids)
    log.info("Image %d processed, final centroids (iterations: %d): %s.".format(SeqKMeans.incrementAndGet, iter.get(), result))
    (result, iter.get())
  }

  protected final def converged(oldNew: (Seq[(PointColour, PointColour)])): Boolean = {
    !(oldNew exists { case (a, b) => (a dist b) > delta })
  }

  /**
    * Sequential KMeans implementation.
    */
  @tailrec
  private def iterate(points: Seq[PointColour], means: Seq[PointColour]): Seq[PointColour] = {
    log.info("Means changed (iterations: %d): %s.".format(iter.getAndIncrement(), means))
    // Repeat old means to preserve groupBy order
    val oldNew = points.groupBy(_ closest means).map { case (mean, cluster) => mean -> PointColour.average(cluster) }.toSeq
    if (converged(oldNew)) oldNew.unzip._2 else iterate(points, oldNew.unzip._2)
  }

  override def clone(): SeqKMeans = new SeqKMeans(seeder)

}

object SeqKMeans{
  private val imageCount = new AtomicLong(0)
  def incrementAndGet: Long = imageCount.incrementAndGet()
}