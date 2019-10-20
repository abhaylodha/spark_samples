package my.spark.common_utils

import org.apache.spark.sql.SparkSession

trait SparkRunner extends Logger {
  def main(args: Array[String]): Unit = {

    val appName = args.head
    val mode = args.tail.head

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

    run(spark: SparkSession, args.tail.tail)

    info("Application processing completed.")

  }

  def run(spark: SparkSession, args: Array[String]): Unit

}