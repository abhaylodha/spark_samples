package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, collect_set, struct }
import scala.util.Random
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.parquet.format.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import scala.collection.mutable.WrappedArray
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

/**
 *
 * Problem statement : Group by orderuuid, ordervid, side and
 * if side is buy (1), sort by bp in descending, ap in ascending and get 10th record.
 * if side is sell (2), sort by ap in ascending, bp in descending and get 10th record.
 *
 */
object DataFrameMapUse extends SparkRunner {
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {
    val r = new Random

    val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)

    import sqlContext.implicits._

    val data: DataFrame = ((for (i <- 1 to 50) yield {
      ("O" + (r.nextInt(5) + 1),
        "V" + (r.nextInt(3) + 1), "1", r.nextInt(100), r.nextInt(100), r.nextInt(100), r.nextInt(100))
    }).toSeq ++
      (for (i <- 1 to 50) yield {
        ("O" + (r.nextInt(5) + 6),
          "V" + (r.nextInt(3) + 1), "2", r.nextInt(100), r.nextInt(100), r.nextInt(100), r.nextInt(100))
      }).toSeq).toDF("orderuuid", "ordervid", "side", "ap", "bp", "as", "bs")

    data.printSchema
    /**
     * root
     * |-- orderuuid: string (nullable = true)
     * |-- ordervid: string (nullable = true)
     * |-- ap: integer (nullable = false)
     * |-- bp: integer (nullable = false)
     */

    data.schema("orderuuid")

    val encoder = RowEncoder(StructType(
      data.schema("orderuuid") ::
        data.schema("ordervid") ::
        data.schema("side") ::
        data.schema("ap") ::
        data.schema("bp") ::
        data.schema("as") ::
        data.schema("bs") ::
        Nil))

    val data1 = data
      .groupBy(col("orderuuid"), col("ordervid"), col("side"))
      .agg(collect_set(struct("ap", "bp", "as", "bs")).as("mkt_data"))
    val data2 = data1.mapPartitions(rowIterator(10))(encoder)

    info("Sorted data")
    data.orderBy(col("orderuuid"), col("ordervid"),
      col("side"), col("bp").desc, col("ap"),
      col("as"), col("bs"))
      .show(100, false)

    info("Summary data")
    //data1.show(100, false)
    data2.show(100, false)

  }

  def rowIterator(depth: Int)(iterator: Iterator[Row]) = {
    def getMaxBPMinAP(price_info: WrappedArray[GenericRowWithSchema]) = {
      val d = price_info.map(r => (r.getAs[Int]("ap"), r.getAs[Int]("bp"),
        r.getAs[Int]("as"), r.getAs[Int]("bs")))
      val d1 = d.sortWith((lh, rh) => lh._2 > rh._2 || (lh._2 == rh._2 && lh._1 < rh._1)).take(depth).last
      d1
    }

    def getMinAPMaxBP(price_info: WrappedArray[GenericRowWithSchema]) = {
      val d = price_info.map(r => (r.getAs[Int]("ap"), r.getAs[Int]("bp"),
        r.getAs[Int]("as"), r.getAs[Int]("bs")))
      d.sortWith((lh, rh) => lh._1 < rh._1 || (lh._1 == rh._1 && lh._2 > rh._2)).take(depth).last
    }

    iterator.map(r => {

      val side = r.getAs[String]("side")
      val (ap, bp, as, bs) = if (side == "1")
        getMaxBPMinAP(r.getAs[WrappedArray[GenericRowWithSchema]]("mkt_data"))
      else
        getMinAPMaxBP(r.getAs[WrappedArray[GenericRowWithSchema]]("mkt_data"))

      Row(
        r.getAs[Int]("orderuuid"),
        r.getAs[Int]("ordervid"),
        side,
        ap,
        bp,
        as,
        bs)
    })

  }
}
