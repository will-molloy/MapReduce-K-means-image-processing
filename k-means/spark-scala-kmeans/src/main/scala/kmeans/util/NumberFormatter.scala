package kmeans.util

import java.text.NumberFormat

object NumberFormatter {

  private val nf = NumberFormat.getInstance()

  def apply(x: Long): String = nf.format(x)

}
