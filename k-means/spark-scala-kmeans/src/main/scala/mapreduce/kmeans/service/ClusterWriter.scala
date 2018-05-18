package mapreduce.kmeans.service

import java.io.BufferedWriter
import java.util.concurrent.atomic.AtomicInteger

import mapreduce.kmeans.model.VectorPoint

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ClusterWriter(val writer: BufferedWriter) {

  private val atom: AtomicInteger = new AtomicInteger(0)

  /**
    * Method to print the clusters for plotting
    */
  def printClusters(centroids: Seq[VectorPoint], points: Seq[VectorPoint]): Unit = {
    val dict = new mutable.HashMap[VectorPoint, ArrayBuffer[VectorPoint]]()
    centroids.foreach { e =>
      dict.put(e, new ArrayBuffer[VectorPoint]())
    }
    points.foreach { e =>
      val a = dict.get(e ?? centroids)
      a.get += e
    }

    def writeSeq(a : Seq[Any]): Unit = {
      writer.write("[")
      for ((i,n) <- a.zip(0 to a.size)) {
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
