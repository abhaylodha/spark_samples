package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, lit }

object Test1 extends SparkRunner {
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {
    import spark.sqlContext.implicits._
    val data1 = Seq(
      ("str1", "1", "1.1", "2019-11-30 01:02:03.156335"),
      ("str2", "2", "2.2", "2019-12-01 04:03:04.256355"),
      ("str3", "3", "3.3", "2019-12-02 05:43:05.336365"),
      ("str4", "4", "4.4", "2019-12-03 06:05:06.416375"))
      .toDF("c1", "c2", "c3", "c4")

    info("Data filter with null value")
    data1.filter(lit(null) === "abc").show(false)
    data1.filter(lit(null) =!= "abc").show(false)

  }

}