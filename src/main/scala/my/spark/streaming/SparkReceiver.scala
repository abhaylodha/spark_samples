package my.spark.streaming

import my.spark.common_utils.Logger
import org.apache.spark.sql.SparkSession
import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, split }
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row

object SparkReceiver extends SparkRunner with Logger {

  /**
   * Entry point of the program
   */
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .withColumn("splitted_values", split(col("value"), ","))
      .withColumn("date_formatted", col("splitted_values")(0))
      .withColumn("event_no", col("splitted_values")(1))
      .withColumn("temperature", col("splitted_values")(2))
      .drop("value", "splitted_values")

    val console_output = df
      .writeStream
      .foreach(new ForeachWriter[Row] {

        @Override
        def open(partitionId: Long, version: Long): Boolean = {
          println("Started writing")
          true
        }

        @Override
        def process(record: Row) = {
          // write string to connection
          println(s"Got the record : $record")
        }

        @Override
        def close(errorOrNull: Throwable): Unit = {
          // close the connection
          println("Closing the connection")
        }
      })
      //.format("console")
      .start()

    spark.streams.awaitAnyTermination()

  }
}