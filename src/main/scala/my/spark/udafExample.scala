package my.spark

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.Row
import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Input :
 * +---+------------+-------------------+
 * |id |command_type|system_create_ts   |
 * +---+------------+-------------------+
 * |1  |New         |2020-12-01 01:00:01|
 * |1  |Amend       |2020-12-01 01:01:01|
 * |1  |Amend       |2020-12-01 01:02:01|
 * |1  |Amend       |2020-12-01 01:03:01|
 * |1  |Amend       |2020-12-01 01:04:01|
 * |1  |Cancelled   |2020-12-01 01:05:01|
 * |2  |New         |2020-12-01 01:01:01|
 * |2  |Amend       |2020-12-01 01:02:01|
 * |2  |Amend       |2020-12-01 01:03:01|
 * |2  |Amend       |2020-12-01 01:04:01|
 * |2  |Amend       |2020-12-01 01:05:01|
 * |2  |Cancelled   |2020-12-01 01:06:01|
 * |3  |New         |2020-12-01 02:01:01|
 * |4  |New         |2020-12-01 01:01:01|
 * |5  |New         |2020-12-01 07:01:01|
 * |6  |New         |2020-12-01 04:01:01|
 * +---+------------+-------------------+
 *
 *
 * Output:
 * +---+--------------------+----------------+--------------------+----------------+
 * |id |min_system_create_ts|min_command_type|max_system_create_ts|max_command_type|
 * +---+--------------------+----------------+--------------------+----------------+
 * |1  |2020-12-01 01:00:01 |New             |2020-12-01 01:05:01 |Cancelled       |
 * |6  |2020-12-01 04:01:01 |New             |2020-12-01 04:01:01 |New             |
 * |3  |2020-12-01 02:01:01 |New             |2020-12-01 02:01:01 |New             |
 * |5  |2020-12-01 07:01:01 |New             |2020-12-01 07:01:01 |New             |
 * |4  |2020-12-01 01:01:01 |New             |2020-12-01 01:01:01 |New             |
 * |2  |2020-12-01 01:01:01 |New             |2020-12-01 01:06:01 |Cancelled       |
 * +---+--------------------+----------------+--------------------+----------------+
 */

private class FindFirstAndLastCmdTypeCode extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(Array(
    StructField("system_create_ts", TimestampType),
    StructField("command_type", StringType)))

  def bufferSchema: StructType = StructType(Array(
    StructField("min_system_create_ts", TimestampType),
    StructField("min_command_type", StringType),
    StructField("max_system_create_ts", TimestampType),
    StructField("max_command_type", StringType)))

  def dataType: DataType = bufferSchema

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = null
    buffer(1) = ""
    buffer(2) = null
    buffer(3) = ""
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val new_system_create_ts = input.getTimestamp(0)
    val new_command_type = input.getString(1)

    val current_min_system_create_ts = buffer.getTimestamp(0)
    val current_min_command_type = buffer.getString(1)
    val current_max_system_create_ts = buffer.getTimestamp(2)
    val current_max_command_type = buffer.getString(3)

    if (current_min_system_create_ts == null) {
      buffer(0) = new_system_create_ts
      buffer(1) = new_command_type
      buffer(2) = new_system_create_ts
      buffer(3) = new_command_type

    } else {
      if (new_system_create_ts.before(current_min_system_create_ts)) {
        buffer(0) = new_system_create_ts
        buffer(1) = new_command_type
      }
      if (new_system_create_ts.after(current_max_system_create_ts)) {
        buffer(2) = new_system_create_ts
        buffer(3) = new_command_type
      }
    }

  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val current_min_system_create_ts = buffer1.getTimestamp(0)
    val current_min_command_type = buffer1.getString(1)
    val current_max_system_create_ts = buffer1.getTimestamp(2)
    val current_max_command_type = buffer1.getString(3)

    val new_min_system_create_ts = buffer2.getTimestamp(0)
    val new_min_command_type = buffer2.getString(1)
    val new_max_system_create_ts = buffer2.getTimestamp(2)
    val new_max_command_type = buffer2.getString(3)

    if (current_min_system_create_ts == null ||
      new_min_system_create_ts.before(current_min_system_create_ts)) {
      buffer1(0) = new_min_system_create_ts
      buffer1(1) = new_min_command_type
    }

    if (current_max_system_create_ts == null ||
      new_max_system_create_ts.after(current_max_system_create_ts)) {
      buffer1(2) = new_max_system_create_ts
      buffer1(3) = new_max_command_type
    }

    println("buffer 0: " + buffer1.getTimestamp(0))
    println("buffer 1: " + buffer1.getString(1))
    println("buffer 2: " + buffer1.getTimestamp(2))
    println("buffer 3: " + buffer1.getString(3))

  }

  def evaluate(buffer: Row): Any = {
    (
      buffer.getTimestamp(0),
      buffer.getString(1),
      buffer.getTimestamp(2),
      buffer.getString(3))
  }

}

object udafExample extends SparkRunner {
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {
    import spark.implicits._
    val data = Seq(
      (1, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 1, 1))),
      (1, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 4, 1))),
      (1, "Cancelled", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 5, 1))),
      (1, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 2, 1))),
      (1, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 3, 1))),
      (1, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 0, 1))),

      (2, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 5, 1))),
      (2, "Cancelled", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 6, 1))),
      (2, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 4, 1))),
      (2, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 1, 1))),
      (2, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 2, 1))),
      (2, "Amend", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 3, 1))),

      (3, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 2, 1, 1))),
      (4, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 1, 1, 1))),
      (5, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 7, 1, 1))),
      (6, "New", Timestamp.valueOf(LocalDateTime.of(2020, 12, 1, 4, 1, 1)))).toDF("id", "command_type", "system_create_ts")

    val udaf = new FindFirstAndLastCmdTypeCode
    val data1 = data
      .groupBy("id").agg(udaf(col("system_create_ts"), col("command_type")).as("agg_values"))
      .withColumn("min_system_create_ts", col("agg_values.min_system_create_ts"))
      .withColumn("min_command_type", col("agg_values.min_command_type"))
      .withColumn("max_system_create_ts", col("agg_values.max_system_create_ts"))
      .withColumn("max_command_type", col("agg_values.max_command_type"))
      .drop("agg_values")

    data.orderBy("id", "system_create_ts").show(false)
    data1.show(false)
  }

}
