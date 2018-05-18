package mapreduce.kmeans.model

sealed case class PointColour(red: Double, green: Double, blue: Double) {

  def +(that: PointColour) = PointColour(red + that.red, green + that.green, blue + that.blue)

  def /(that: Double) = PointColour(red / that, green / that, blue / that)

  def ??(that: Seq[PointColour]): PointColour = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)

  def ><(that: PointColour): Double = !(this - that)

  def -(that: PointColour) = PointColour(red - that.red, green - that.green, blue - that.blue)

  def unary_!(): Double = math.sqrt(red * red + blue * blue + green * green)

  override def toString = s"$red $green $blue"
}

object PointColour {
  def random(): PointColour = PointColour(math random, math random, math random)

  def origin(): PointColour = PointColour(0, 0, 0)
}