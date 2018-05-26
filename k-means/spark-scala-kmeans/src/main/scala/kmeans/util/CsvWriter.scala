package kmeans.util

import java.io.{BufferedReader, File, FileReader, FileWriter}

import kmeans.Main.{logFile, csvFile}

import scala.collection.JavaConverters._

/**
  * Write log file to csv file
  */
object CsvWriter {

  def main(args: Array[String]): Unit ={
    val reader = new BufferedReader(new FileReader(new File(logFile)))
    val writer = new FileWriter(new File(csvFile)) // overwrite

    reader.mark(100)
    val header = extract(reader.readLine(), 0)
    reader.reset()

    writer.write(header)
    reader.lines().iterator().asScala.foreach(line => writer.write(extract(line, 1)))
    writer.flush()
    writer.close()
  }

  private def extract(entry: String, side: Int): String = {
    val builder = new StringBuilder
    entry.split(",").foreach(attribute => builder.append(attribute.split(":")(side)).append(","))
    builder.toString + "\n"
  }

}
