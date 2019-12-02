package my.spark

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

object DataFrameSchemaChanger extends SparkRunner {
  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    import spark.sqlContext.implicits._
    val data1 = Seq(
      ("str1", "1", "1.1", "2019-11-30 01:02:03.156335"),
      ("str2", "2", "2.2", "2019-12-01 04:03:04.256355"),
      ("str3", "3", "3.3", "2019-12-02 05:43:05.336365"),
      ("str4", "4", "4.4", "2019-12-03 06:05:06.416375"))
      .toDF("c1", "c2", "c3", "c4")

    info("Data with all columns as String")
    data1.printSchema
    data1.show(false)

    val data2 = Seq(
      ("str1", 1, 1.1, "2019-11-30 01:02:03.156335"))
      .toDF("c1", "c2", "c3", "c4")
      .withColumn("c3", col("c3").cast("decimal(30,12)"))
      .withColumn("c4", col("c4").cast("timestamp"))

    info("Data with all columns as Actual Type")
    data2.printSchema
    data2.show(false)

    val finalData = mapSchema(data1, data2)

    info("Data with all columns as Required Type")
    finalData.printSchema
    finalData.show(false)

  }

  def mapSchema(dfWithData: DataFrame, dfWithRequiredSchema: DataFrame): DataFrame = {

    val fields = dfWithRequiredSchema.schema.fields

    fields.foldLeft(dfWithData)((df, field) =>
      df.withColumn(field.name, col(field.name).cast(field.dataType)))
  }
}

/**

05:57:24 INFO DataFrameSchemaChanger$: Data with all columns as String
root
 |-- c1: string (nullable = true)
 |-- c2: string (nullable = true)
 |-- c3: string (nullable = true)
 |-- c4: string (nullable = true)

+----+---+---+--------------------------+
|c1  |c2 |c3 |c4                        |
+----+---+---+--------------------------+
|str1|1  |1.1|2019-11-30 01:02:03.156335|
|str2|2  |2.2|2019-12-01 04:03:04.256355|
|str3|3  |3.3|2019-12-02 05:43:05.336365|
|str4|4  |4.4|2019-12-03 06:05:06.416375|
+----+---+---+--------------------------+

05:57:25 INFO DataFrameSchemaChanger$: Data with all columns as Actual Type
root
 |-- c1: string (nullable = true)
 |-- c2: integer (nullable = false)
 |-- c3: decimal(30,12) (nullable = true)
 |-- c4: timestamp (nullable = true)

+----+---+--------------+--------------------------+
|c1  |c2 |c3            |c4                        |
+----+---+--------------+--------------------------+
|str1|1  |1.100000000000|2019-11-30 01:02:03.156335|
+----+---+--------------+--------------------------+

05:57:25 INFO DataFrameSchemaChanger$: Data with all columns as Required Type
root
 |-- c1: string (nullable = true)
 |-- c2: integer (nullable = true)
 |-- c3: decimal(30,12) (nullable = true)
 |-- c4: timestamp (nullable = true)

+----+---+--------------+--------------------------+
|c1  |c2 |c3            |c4                        |
+----+---+--------------+--------------------------+
|str1|1  |1.100000000000|2019-11-30 01:02:03.156335|
|str2|2  |2.200000000000|2019-12-01 04:03:04.256355|
|str3|3  |3.300000000000|2019-12-02 05:43:05.336365|
|str4|4  |4.400000000000|2019-12-03 06:05:06.416375|
+----+---+--------------+--------------------------+

05:57:25 INFO DataFrameSchemaChanger$: Application processing completed.
*/
