package mapreduce.kmeans.service

import mapreduce.kmeans.model.VectorPoint

trait KMeans {

  /**
    * Given a list of points, find k centroids such that the squared distances between each point and their closest
    * centroid is minimised.
    *
    * Since finding the optimal solution is NP-Hard, this is a heuristic and will iterate until the difference between
    * the means converge to less than the given delta.
    */
  def kMeans(points: Array[VectorPoint], kClusters: Int, delta: Double): Seq[VectorPoint]

}
