package mapreduce.kmeans.model

sealed case class VectorPoint(len: Int, private val fromList: List[Double]) {

  def +(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x + y })

  def /(that: Double) = VectorPoint(this.fromList.map { x => x / that })

  /**
    * Magnitude of the point
    */
  def unary_-() = VectorPoint(this.fromList.map { x => -x })

  /**
    * Find the nearest centroid for the given point
    */
  def ??(that: Seq[VectorPoint]): VectorPoint = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)

  /**
    * The euclidean distance between the two points
    */
  def ><(that: VectorPoint): Double = !(this - that)

  def -(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x - y })

  def unary_!(): Double = math.sqrt(this.fromList.map { x => x * x }.sum)

  override def toString: String = fromList.toString()
}

object VectorPoint {
  def apply(list: List[Double]): VectorPoint = this (list.length, list)

  def apply(arr: Array[Double]): VectorPoint = this (arr.length, arr toList)

  def apply(arr: Array[Float]): VectorPoint = this (arr.length, (arr toList) map (x => x toDouble))
}
