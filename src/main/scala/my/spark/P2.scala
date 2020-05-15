package my.spark

import my.spark.common_utils._

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ udf, col }
import org.slf4j.LoggerFactory
import my.spark.common_utils.Logger

object P2 extends Logger{

  val u = udf((n1: Int, n2: Int) => {
    Test.fn(n1, n2)
  })

  def main(args: Array[String]) = {

    val spark: SparkSession = SparkSession.builder().appName("SparkApp")
      .config("spark.master", "local").getOrCreate()

    import spark.sqlContext.implicits._
    val t: DataFrame = Seq((1, 2), (3, 4)).toDF("n1", "n2")
      .withColumn("sum", u(col("n1"), col("n2")))

    info("Full Data")
    warn("Full Data")
    error("Full Data")
    debug("Full Data")
    t.show(false)

    spark.stop()
  }

}
