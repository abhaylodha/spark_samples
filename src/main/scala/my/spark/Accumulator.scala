package my.spark

import my.spark.common_utils.SparkRunner
import my.spark.common_utils.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object Accumulator extends SparkRunner with Logger {
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val df = Seq(
      ("2011-12", 37902),
      ("2012-13", 41401),
      ("2013-14", 46427),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646),
      ("2014-15", 52646))
      .toDF("duration", "AndhraPradesh")

    info("Partition Count: " + df.rdd.partitions.size)
    info("Data Count : " + getCount(spark, df))

    df.show(false)

  }

  def getCount(spark: SparkSession, df: DataFrame) = {
    val dataCounter = spark.sparkContext.longAccumulator("Couner")
    df.foreach(row => { dataCounter.add(1) })
    dataCounter.value
  }
}