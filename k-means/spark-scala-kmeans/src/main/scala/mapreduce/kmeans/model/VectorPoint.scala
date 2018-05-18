package mapreduce.kmeans.model

sealed case class VectorPoint(len: Int, private val fromList: List[Float]) {

  def +(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x + y })

  def /(that: Float) = VectorPoint(this.fromList.map { x => x / that })

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
  def ><(that: VectorPoint): Float = !(this - that)

  def -(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x - y })

  def unary_!(): Float = math.sqrt(this.fromList.map { x => x * x }.sum) toFloat

  override def toString: String = fromList.toString()
}

object VectorPoint {
  def apply(list: List[Float]): VectorPoint = this (list.length, list)

  def apply(arr: Array[Float]): VectorPoint = this (arr.length, arr toList)
}
