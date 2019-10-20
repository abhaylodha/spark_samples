package my.spark

import my.spark.common_utils._

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import java.io.File
import my.spark.common_utils.Logger
import org.apache.spark.sql.Row

object P1 extends Logger {

  def main(args: Array[String]) = {

    info("start of program.")

    val spark: SparkSession =
      try {
        SparkSession
          .builder
          .appName("SparkApp")
          .config("spark.master", "local")
          .getOrCreate()
      } catch {
        case e: Exception => {
          print(e.getMessage)
          SparkSession.builder().appName("SparkApp").config("spark.master", "local").getOrCreate()
        }
      }

    val textFile: Dataset[Row] = spark.read.textFile(args(0)).toDF("id")

    textFile.show(50, false)

    System.out.println("Number of lines: " + textFile.count());
    spark.stop();

  }

}
