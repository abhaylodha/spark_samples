package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, collect_set, struct, udf, lit }
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
 * Problem statement : Group by order_id and create an array of items in the order.
 *
 * Define an udf to -
 *
 * Select only specific columns from the List.
 * Use case class to return result.
 *
 * Ex -
 *
 * Input Data :
 * +--------+------------------------------------------------+
 * |order_id|items                                           |
 * +--------+------------------------------------------------+
 * |1       |[[Anacin, 2, 3.0, 6.0], [Dog Food, 1, 2.5, 2.5]]|
 * |2       |[[Pain Balm, 5, 10.0, 50.0]]                    |
 * +--------+------------------------------------------------+
 *
 * Input Schema :
 * root
 * |-- order_id: integer (nullable = false)
 * |-- items: array (nullable = true)
 * |    |-- element: struct (containsNull = true)
 * |    |    |-- product_name: string (nullable = true)
 * |    |    |-- qty: integer (nullable = false)
 * |    |    |-- price: double (nullable = false)
 * |    |    |-- amount: double (nullable = false)
 *
 * Output Data :
 *
 * +--------+--------------------------------------+
 * |order_id|items                                 |
 * +--------+--------------------------------------+
 * |1       |[[Anacin, 2, 6.0], [Dog Food, 1, 2.5]]|
 * |2       |[[Pain Balm, 5, 50.0]]                |
 * +--------+--------------------------------------+
 *
 * Output Schema :
 *
 * root
 * |-- order_id: integer (nullable = false)
 * |-- items: array (nullable = true)
 * |    |-- element: struct (containsNull = true)
 * |    |    |-- product_name: string (nullable = true)
 * |    |    |-- qty: integer (nullable = true)
 * |    |    |-- amount: double (nullable = false)
 *
 */
object SelectColumnsFromGenericRowWithSchema extends SparkRunner {

  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.sqlContext.implicits._
    val data = Seq(
      (1, "Dog Food", 1, 2.5, 2.5),
      (1, "Anacin", 2, 3.0, 6.0),
      (2, "Pain Balm", 5, 10.0, 50.0))
      .toDF("order_id", "product_name", "qty", "price", "amount")
      .groupBy("order_id").agg(collect_set(struct("product_name", "qty", "price", "amount")).as("items"))
      .transform(df => {
        info("Input")
        df.show(false)
        df.printSchema

        df
      })
      .withColumn("items", selectPartialColumns(col("items")))

    info("Output")
    data.show(false)
    data.printSchema
  }

  val selectPartialColumns = udf((items: WrappedArray[GenericRowWithSchema]) => {
    items.map(Item(_))
  })
}

case class Item(
  product_name: String,
  qty: Integer,
  amount: Double)

object Item {
  def apply(r: GenericRowWithSchema): Item = {
    Item(
      r.getAs[String]("product_name"),
      r.getAs[Integer]("qty"),
      r.getAs[Double]("amount"))
  }
}