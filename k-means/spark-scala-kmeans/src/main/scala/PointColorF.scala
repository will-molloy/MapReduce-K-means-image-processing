sealed case class PointColorF(r: Double, g: Double, b: Double) {
  def + (that : PointColorF) = PointColorF(r + that.r, g + that.g, b + that.b)
  def - (that : PointColorF) = PointColorF(r - that.r, g - that.g, b - that.b)
  def / (that : Double) = PointColorF((r / that) , (g / that), (b / that))

  /**
    * Magnitude of the point
    * @return
    */
  def unary_! () = math.sqrt(r * r + b * b + g * g)

  /**
    * The euclidean distance between the two points
    * @param that
    * @return
    */
  def >< (that: PointColorF)  = !(this - that)

  /**
    * Find the nearest centroid for the given point
    * @param that The centroids
    * @return
    */
  def ?? (that: Seq[PointColorF]) = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)
  override def toString = s"[$r,$g,$b]"
}

object PointColorF {
  def random(): PointColorF = PointColorF(math.random, math.random, math.random)
  def origin(): PointColorF = PointColorF(0, 0, 0)
}

