sealed case class VectorPoint(len: Integer, private val fromList: List[Double]) {

  def +(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x + y })

  def /(that: Double) = VectorPoint(this.fromList.map { x => x / that })

  def unary_-() = VectorPoint(this.fromList.map { x => -x })

  def ??(that: Seq[VectorPoint]): VectorPoint = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)

  def ><(that: VectorPoint): Double = !(this - that)

  def -(that: VectorPoint) = VectorPoint(this.fromList.zip(that.fromList).map { case (x, y) => x - y })

  def unary_!(): Double = math.sqrt(this.fromList.map { x => x * x }.sum)

  override def toString: String = fromList.toString()
}

object VectorPoint {
  def apply(list: List[Double]): VectorPoint = this (list.length, list)

  def random(length: Integer): VectorPoint = VectorPoint(Array.fill(length) {
    math.random
  })

  def apply(arr: Array[Double]): VectorPoint = this (arr.length, arr.toList)
}