import org.apache.spark.sql.SparkSession

object SparkRedditMr {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: binname <inputfile>")
      System.exit(-1)
    }

    val inputFile = args(0)

    val ss = SparkSession.builder().appName("Spark Reddit").getOrCreate()

    import ss.implicits._

    val dataframe = ss.read.json(inputFile).cache()

    dataframe.groupBy($"subreddit").avg("score").show(999)

  }
}
