package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{
  col,
  collect_set,
  array_contains,
  struct,
  lit,
  when,
  row_number,
  min,
  explode
}
import my.spark.common_utils.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import scala.annotation.tailrec

object CommonGroup extends SparkRunner with Logger {
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val df = Seq(
      ("TR1", "TG1"),
      ("TR2", "TG6"),
      ("TR1", "TG2"),
      ("TR3", "TG1"),
      ("TR4", "TG1"),
      ("TR5", "TG2"),
      ("TR6", "TG6"),
      ("TR7", "TG2"),
      ("TR8", "TG1"),
      ("TR9", "TG9"),
      ("TR9", "TG10"),
      ("TR10", "TG10"),
      ("TR10", "TG11"),
      ("TR11", "TG11"),
      ("TR11", "TG12"),
      ("TR12", "TG12")).toDF("trade_ref_id", "trade_ref_group")

    df.show(false)

    val groupIdDF = df.groupBy("trade_ref_group")
      .agg(collect_set(col("trade_ref_id")).as("trade_ref_id"))
      .transform(d => { d.show(false); d })
      .withColumn("group_id", row_number.over(Window.orderBy("trade_ref_group")))
      .withColumn("trade_ref_id", explode(col("trade_ref_id")))

    groupIdDF.show(false)

    //updateGroupId(groupIdDF).show(false)

    val counts = Seq("trade_ref_id", "trade_ref_group")
      .foldLeft(Map[String, Long]())((counts, column) => {
        val count = groupIdDF.select(column).distinct().count
        counts + (column -> count)
      })

    info(s"Count : $counts")

    val finalDF = checkAndUpdateGroup(groupIdDF, counts)

    info("Final DF :")
    finalDF.show(false)
  }

  @tailrec
  def checkAndUpdateGroup(groupIdDF: DataFrame, counts: Map[String, Long]): DataFrame = {
    if (checkCountMatch(groupIdDF, counts)) {
      groupIdDF
    } else {
      info("Still to optimize grouping")
      checkAndUpdateGroup(updateGroupId(groupIdDF), counts)
    }
  }

  def checkCountMatch(groupIdDF: DataFrame, counts: Map[String, Long]): Boolean = {
    info("Checking Count")
    counts.foldLeft(true)((value, currentCheck) => {
      if (value) {
        val count = groupIdDF.select(col(currentCheck._1).as("id"), col("group_id")).distinct().count()
        info(s"${currentCheck._1} Count : $count")
        currentCheck._2 == count
      } else {
        value
      }
    })
  }

  def updateGroupId(groupIdDF: DataFrame) = {
    val groupIdDF2 = groupIdDF.groupBy("trade_ref_id")
      .agg(
        collect_set(col("trade_ref_group")).as("trade_ref_group"),
        min("group_id").as("group_id"))
      .transform(d => { d.show(false); d })
      .withColumn("trade_ref_group", explode(col("trade_ref_group")))

    groupIdDF2.show(false)

    val groupIdDF3 = groupIdDF2.groupBy("trade_ref_group")
      .agg(
        collect_set(col("trade_ref_id")).as("trade_ref_id"),
        min("group_id").as("group_id"))
      .withColumn("trade_ref_id", explode(col("trade_ref_id")))
      .orderBy("group_id")

    groupIdDF3

  }

  /**
   * Input :
   * +------------+---------------+
   * |trade_ref_id|trade_ref_group|
   * +------------+---------------+
   * |TR1         |TG1            |
   * |TR2         |TG6            |
   * |TR1         |TG2            |
   * |TR3         |TG1            |
   * |TR4         |TG1            |
   * |TR5         |TG2            |
   * |TR6         |TG6            |
   * |TR7         |TG2            |
   * |TR8         |TG1            |
   * |TR9         |TG9            |
   * |TR9         |TG10           |
   * |TR10        |TG10           |
   * |TR10        |TG11           |
   * |TR11        |TG11           |
   * |TR11        |TG12           |
   * |TR12        |TG12           |
   * +------------+---------------+
   * Count : Map(trade_ref_id -> 12, trade_ref_group -> 7)
   *
   * Output :
   * +---------------+------------+--------+
   * |trade_ref_group|trade_ref_id|group_id|
   * +---------------+------------+--------+
   * |TG1            |TR4         |1       |
   * |TG1            |TR1         |1       |
   * |TG2            |TR7         |1       |
   * |TG2            |TR1         |1       |
   * |TG1            |TR3         |1       |
   * |TG2            |TR5         |1       |
   * |TG1            |TR8         |1       |
   * |TG12           |TR11        |2       |
   * |TG12           |TR12        |2       |
   * |TG11           |TR11        |2       |
   * |TG10           |TR9         |2       |
   * |TG10           |TR10        |2       |
   * |TG11           |TR10        |2       |
   * |TG9            |TR9         |2       |
   * |TG6            |TR2         |6       |
   * |TG6            |TR6         |6       |
   * +---------------+------------+--------+
   *
   */

}
