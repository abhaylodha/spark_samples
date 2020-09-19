package my.spark

import my.spark.common_utils.SparkRunner
import my.spark.common_utils.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.DoubleAccumulator
import org.apache.spark.util.CollectionAccumulator

case class MyClass(a: String, b: Int)

object StandardAccumulators extends SparkRunner with Logger {
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val longAcc = spark.sparkContext.longAccumulator("longAcc")
    val doubleAcc = spark.sparkContext.doubleAccumulator("doubleAcc")
    val collectionAcc = spark.sparkContext.collectionAccumulator[MyClass]("collectionAcc")

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
      .transform(getAccValues(longAcc, doubleAcc, collectionAcc))

    info("longAcc before action will have zero value : " + longAcc.value)
    info("longAcc before action will have zero count : " + longAcc.count)
    info("longAcc before action will have zero sum   : " + longAcc.sum)

    info("doubleAcc before action will have zero value : " + doubleAcc.value)
    info("doubleAcc before action will have zero count : " + doubleAcc.count)
    info("doubleAcc before action will have zero sum   : " + doubleAcc.sum)

    info("collectionAcc before action will have no value : " + collectionAcc.value)
    info("collectionAcc before action will have zero count : " + collectionAcc.value.size)

    df.show(false)

    info("longAcc after action will have zero value : " + longAcc.value)
    info("longAcc after action will have zero count : " + longAcc.count)
    info("longAcc after action will have zero sum   : " + longAcc.sum)

    info("doubleAcc after action will have zero value : " + doubleAcc.value)
    info("doubleAcc after action will have zero count : " + doubleAcc.count)
    info("doubleAcc after action will have zero sum   : " + doubleAcc.sum)

    info("collectionAcc after action will have no value : " + collectionAcc.value)
    info("collectionAcc after action will have zero count : " + collectionAcc.value.size)

  }

  def getAccValues(
    longAcc: LongAccumulator,
    doubleAcc: DoubleAccumulator,
    collectionAcc: CollectionAccumulator[MyClass])(df: DataFrame): DataFrame = {
    val df1 = df.mapPartitions((rows: Iterator[Row]) => {
      rows.map(row =>
        {
          longAcc.add(2)
          doubleAcc.add(row.getAs[Int](1))
          collectionAcc.add(MyClass(row.getAs[String](0), row.getAs[Int](1)))
          row
        })
    })(RowEncoder(df.schema))
    df1
  }
}