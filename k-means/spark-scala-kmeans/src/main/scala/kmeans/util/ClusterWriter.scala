package kmeans.util

import java.io.BufferedWriter
import java.util.concurrent.atomic.AtomicInteger

import kmeans.model.PointColour

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object ClusterWriter {

  private val atom: AtomicInteger = new AtomicInteger(0)

  /**
    * Method to print the clusters for plotting
    */
  def write(writer: BufferedWriter, centroids: Seq[PointColour], points: Seq[PointColour]): Unit = {
    val dict = new mutable.HashMap[PointColour, ArrayBuffer[PointColour]]()
    centroids.foreach { e =>
      dict.put(e, new ArrayBuffer[PointColour]())
    }
    points.foreach { e =>
      val a = dict.get(e closest centroids)
      a.get += e
    }

    def writeSeq(a: Seq[Any]): Unit = {
      writer.write("[")
      for ((i, n) <- a.zip(0 to a.size)) {
        writer.write(i.toString)
        if (n != a.size - 1) {
          writer.write(",")
        }
        writer.flush()
      }
      writer.write("]")
    }

    writer.write("[")

    val dd = dict.toArray

    for (((k, v), n) <- dd.zip(0 to dd.length)) {
      writer.write(k.toString)
      writer.write(",")
      writeSeq(v)
      if (n != dd.length - 1) writer.write(",")
    }
    writer.write("]")

    writer.flush()
    atom.incrementAndGet()
    writer.close()
  }

}
