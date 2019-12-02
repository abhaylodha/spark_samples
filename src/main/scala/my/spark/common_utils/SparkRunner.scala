package my.spark.common_utils

import org.apache.spark.sql.SparkSession

trait SparkRunner extends Logger {
  def main(args: Array[String]): Unit = {

    val appName = if (args.length <= 0) "Sample Application" else args.head
    val mode = if (args.length <= 0) "local" else args.tail.head

    info(s"Starting App : ${appName}")

    val spark: SparkSession =
      if (mode == "local")
        SparkSession
          .builder
          .appName(appName)
          .config("spark.master", "local[5]")
          .config("spark.ui.enabled", "true")
          .config("spark.cores.max", "2")
          .getOrCreate
      else
        SparkSession
          .builder
          .appName(appName)
          .config("spark.master", "yarn")
          .config("spark.ui.enabled", "true")
          .config("spark.cores.max", "2")
          .getOrCreate

    run(spark: SparkSession, if (args.length <= 0) Array[String]() else args.tail.tail)

    info("Application processing completed.")

  }

  def run(spark: SparkSession, args: Array[String]): Unit

}