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
      ("TR8", "TG1")).toDF("trade_ref_id", "trade_ref_group")

    df.show(false)

    val groupIdDF = df.groupBy("trade_ref_group")
      .agg(collect_set(col("trade_ref_id")).as("trade_ref_id"))
      .transform(d => { d.show(false); d })
      .withColumn("group_id", row_number.over(Window.orderBy("trade_ref_group")))
      .withColumn("trade_ref_id", explode(col("trade_ref_id")))

    groupIdDF.show(false)

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

    groupIdDF3.show(false)

  }
}

  /**
   * 01:05:05 INFO CommonGroup$: Starting App : Sample Application
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
   * +------------+---------------+
   *
   * +---------------+--------------------+
   * |trade_ref_group|trade_ref_id        |
   * +---------------+--------------------+
   * |TG1            |[TR4, TR1, TR3, TR8]|
   * |TG2            |[TR7, TR1, TR5]     |
   * |TG6            |[TR2, TR6]          |
   * +---------------+--------------------+
   *
   * 01:05:21 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
   * +---------------+------------+--------+
   * |trade_ref_group|trade_ref_id|group_id|
   * +---------------+------------+--------+
   * |TG1            |TR4         |1       |
   * |TG1            |TR1         |1       |
   * |TG1            |TR3         |1       |
   * |TG1            |TR8         |1       |
   * |TG2            |TR7         |2       |
   * |TG2            |TR1         |2       |
   * |TG2            |TR5         |2       |
   * |TG6            |TR2         |3       |
   * |TG6            |TR6         |3       |
   * +---------------+------------+--------+
   *
   * 01:05:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
   * +------------+---------------+--------+
   * |trade_ref_id|trade_ref_group|group_id|
   * +------------+---------------+--------+
   * |TR4         |[TG1]          |1       |
   * |TR1         |[TG2, TG1]     |1       |
   * |TR3         |[TG1]          |1       |
   * |TR8         |[TG1]          |1       |
   * |TR7         |[TG2]          |2       |
   * |TR5         |[TG2]          |2       |
   * |TR2         |[TG6]          |3       |
   * |TR6         |[TG6]          |3       |
   * +------------+---------------+--------+
   *
   * 01:05:23 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
   * +------------+---------------+--------+
   * |trade_ref_id|trade_ref_group|group_id|
   * +------------+---------------+--------+
   * |TR4         |TG1            |1       |
   * |TR1         |TG2            |1       |
   * |TR1         |TG1            |1       |
   * |TR3         |TG1            |1       |
   * |TR8         |TG1            |1       |
   * |TR7         |TG2            |2       |
   * |TR5         |TG2            |2       |
   * |TR2         |TG6            |3       |
   * |TR6         |TG6            |3       |
   * +------------+---------------+--------+
   *
   * 01:05:24 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
   * +---------------+------------+--------+
   * |trade_ref_group|trade_ref_id|group_id|
   * +---------------+------------+--------+
   * |TG1            |TR3         |1       |
   * |TG1            |TR4         |1       |
   * |TG2            |TR7         |1       |
   * |TG2            |TR1         |1       |
   * |TG2            |TR5         |1       |
   * |TG1            |TR8         |1       |
   * |TG1            |TR1         |1       |
   * |TG6            |TR2         |3       |
   * |TG6            |TR6         |3       |
   * +---------------+------------+--------+
   *
   * 01:05:25 INFO CommonGroup$: Application processing completed.
   *
   */
