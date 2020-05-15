package my.spark.streaming

import my.spark.common_utils.Logger
import org.apache.spark.sql.SparkSession
import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.DataFrame

object Receiver extends SparkRunner with Logger {

  /**
   * Entry point of the program
   */
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 5000)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    
    words.printSchema

    // Generate running word count
    val wordCounts : DataFrame = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }

}