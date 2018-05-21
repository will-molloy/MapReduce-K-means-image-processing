package kmeans.model

import scala.util.Random

/**
  * Represents pixel of an image.
  * Note: List/Array implementation is much slower.
  */
sealed case class PointColour(r: Float, g: Float, b: Float) {

  def +(that: PointColour) = PointColour(r + that.r, g + that.g, b + that.b)

  def -(that: PointColour) = PointColour(r - that.r, g - that.g, b - that.b)

  def /(div: Float) = PointColour(r / div, g / div, b / div)

  /**
    * Euclidean distance
    */
  def dist(that: PointColour): Double = !(this - that)

  /**
    * Euclidean norm
    */
  private def unary_!(): Double = math.sqrt(r * r + b * b + g * g)

  def distClosest(points: Seq[PointColour]): Double = this dist (this closest points)

  def closest(points: Seq[PointColour]): PointColour = points.reduceLeft((a, b) => if ((this dist a) < (this dist b)) a else b)

  override def toString: String = s"[$r,$g,$b]"
}

object PointColour {
  def uniformlyRandom(points: Seq[PointColour]): PointColour = points(Random.nextInt(points.length))

  def average(points: Seq[PointColour]): PointColour = points.reduce((a, b) => a + b) / points.length
}
