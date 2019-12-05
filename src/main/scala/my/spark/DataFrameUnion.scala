package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object DataFrameUnion extends SparkRunner {

  def myUnion(d1: DataFrame, d2: DataFrame): DataFrame = {
    val startTime = System.currentTimeMillis

    val data = d1.union(d2)

    val endTime = System.currentTimeMillis

    val duration = endTime - startTime

    //info("Duration with DF : " + duration)

    data

  }

  def myUnionWithRDD(spark: SparkSession)(
    d1: DataFrame,
    d2: DataFrame): DataFrame = {
    val startTime = System.currentTimeMillis

    val data = spark.createDataFrame(d1.rdd.union(d2.rdd), d1.schema)

    val endTime = System.currentTimeMillis

    val duration = endTime - startTime

    //info("Duration with RDD : " + duration)
    data
  }

  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    import sqlContext.implicits._

    //val sqlc = spark.sqlContext
    //import sqlc.implicits._

    val data1 = Seq((1, "One"), (2, "Two"), (3, "Thress")).toDF("id", "name")
    val data2 = Seq((1, "One"), (2, "Two"), (3, "Thress")).toDF("id", "name")

    info("With DF Union")
    evaluate(data1, data2, myUnion)

    info("With RDD Union")
    evaluate(data1, data2, myUnionWithRDD(spark))

  }

  def evaluate(
    d1: DataFrame,
    d2: DataFrame,
    unionOp: (DataFrame, DataFrame) => DataFrame) = {
    val startTime = System.currentTimeMillis
    val d3 = (1 to 90).foldLeft(d1)((df, i) =>
      df.transform(unionOp(_, d2)))
    d3.show(false)
    info("Count : " + d3.count)
    val endTime = System.currentTimeMillis
    val duration = endTime - startTime

    info("Duration : " + duration)
  }

}
