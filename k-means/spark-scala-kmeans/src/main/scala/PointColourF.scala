sealed case class PointColourF(red: Double, green: Double, blue: Double) {
  def +(that: PointColourF) = PointColourF(red + that.red, green + that.green, blue + that.blue)

  def /(that: Double) = PointColourF(red / that, green / that, blue / that)

  def ??(that: Seq[PointColourF]): PointColourF = that.reduceLeft((a, b) => if ((this >< a) < (this >< b)) a else b)

  def ><(that: PointColourF): Double = !(this - that)

  def -(that: PointColourF) = PointColourF(red - that.red, green - that.green, blue - that.blue)

  def unary_!(): Double = math.sqrt(red * red + blue * blue + green * green)

  override def toString = s"$red $green $blue"
}

object PointColourF {
  def random(): PointColourF = PointColourF(math.random, math.random, math.random)

  def origin(): PointColourF = PointColourF(0, 0, 0)
}