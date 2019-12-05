package my.spark

import my.spark.common_utils._

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ col, split }
import java.io.File
import my.spark.common_utils.Logger
import org.apache.spark.sql.Row

object Self_Join extends SparkRunner with Logger {
  def run(spark: SparkSession, args: Array[String]): Unit = {
    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val data = Seq(
      ("2011-12", 37902),
      ("2012-13", 41401),
      ("2013-14", 46427),
      ("2014-15", 52646))
      .toDF("duration", "AndhraPradesh")
      .withColumn("durationTemp", split(col("duration"), "-"))
      .withColumn("durationFrom", col("durationTemp")(0).cast("integer"))
      .withColumn("durationTo", col("durationTemp")(1).cast("integer"))

    data.show(false)

    val data1 = data.select(
      col("durationFrom").as("durationFromPrev"),
      col("durationTo").as("durationToPrev"),
      col("AndhraPradesh").as("AndhraPradeshPrev"))
      .join(data, col("durationFrom") === col("durationFromPrev") + 1
        && col("durationTo") === col("durationToPrev") + 1)

    info("Data1")
    data1.show(false)

  }

}