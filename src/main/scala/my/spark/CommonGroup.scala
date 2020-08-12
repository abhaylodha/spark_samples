package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, collect_set, array_contains, struct, lit, when, row_number }
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

    val groupIdDF = df.select(col("trade_ref_group").as("value"), lit(1).as("priority"))
      .union(df.select(col("trade_ref_id").as("value"), lit(2).as("priority"))).distinct()
      .withColumn("group_id", row_number.over(Window.orderBy("priority", "value")))

    groupIdDF.show(false)

    val data = df.join(groupIdDF, col("trade_ref_group") === col("value"))
      .drop("value")
      .withColumnRenamed("group_id", "group_id_1")
      .join(groupIdDF, col("trade_ref_id") === col("value"))
      .drop("value")
      .withColumnRenamed("group_id", "group_id_2")
      .withColumn("group_id", when(col("group_id_1") < col("group_id_2"), col("group_id_1"))
        .otherwise(col("group_id_2")))
      .select("trade_ref_id", "trade_ref_group", "group_id")

    data.show(false)

    val data_v1 = data.alias("d1").join(
      data.alias("d2"),
      col("d1.trade_ref_id") === col("d2.trade_ref_id"))
      .withColumn("group_id_v1", when(col("d1.group_id") < col("d2.group_id"), col("d1.group_id"))
        .otherwise(col("d2.group_id")))
      .select(
        col("d1.trade_ref_id").as("trade_ref_id"),
        col("d1.trade_ref_group").as("trade_ref_group"),
        col("group_id_v1").as("group_id"))
      .distinct

    data_v1.show(false)

    val data_v2 = data_v1.alias("d1").join(
      data_v1.alias("d2"),
      col("d1.trade_ref_group") === col("d2.trade_ref_group"))
      .withColumn("group_id_v2", when(col("d1.group_id") < col("d2.group_id"), col("d1.group_id"))
        .otherwise(col("d2.group_id")))
      .select(
        col("d1.trade_ref_id").as("trade_ref_id"),
        col("d1.trade_ref_group").as("trade_ref_group"),
        col("group_id_v2").as("group_id"))
      .distinct
      .orderBy(col("group_id"))

    data_v2.show(false)

  }
}

/**
 * 23:42:27 INFO CommonGroup$: Starting App : Sample Application
23:42:28 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
+------------+---------------+
|trade_ref_id|trade_ref_group|
+------------+---------------+
|TR1         |TG1            |
|TR2         |TG6            |
|TR1         |TG2            |
|TR3         |TG1            |
|TR4         |TG1            |
|TR5         |TG2            |
|TR6         |TG6            |
|TR7         |TG2            |
|TR8         |TG1            |
+------------+---------------+

23:42:35 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
23:42:42 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
23:42:42 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+-----+--------+--------+
|value|priority|group_id|
+-----+--------+--------+
|TG1  |1       |1       |
|TG2  |1       |2       |
|TG6  |1       |3       |
|TR1  |2       |4       |
|TR2  |2       |5       |
|TR3  |2       |6       |
|TR4  |2       |7       |
|TR5  |2       |8       |
|TR6  |2       |9       |
|TR7  |2       |10      |
|TR8  |2       |11      |
+-----+--------+--------+

+------------+---------------+--------+----------+--------+----------+--------+
|trade_ref_id|trade_ref_group|priority|group_id_1|priority|group_id_2|group_id|
+------------+---------------+--------+----------+--------+----------+--------+
|TR8         |TG1            |1       |1         |2       |11        |1       |
|TR4         |TG1            |1       |1         |2       |7         |1       |
|TR3         |TG1            |1       |1         |2       |6         |1       |
|TR1         |TG1            |1       |1         |2       |4         |1       |
|TR7         |TG2            |1       |2         |2       |10        |2       |
|TR5         |TG2            |1       |2         |2       |8         |2       |
|TR1         |TG2            |1       |2         |2       |4         |2       |
|TR6         |TG6            |1       |3         |2       |9         |3       |
|TR2         |TG6            |1       |3         |2       |5         |3       |
+------------+---------------+--------+----------+--------+----------+--------+
23:42:45 INFO CommonGroup$: Application processing completed.

 * 
 */
