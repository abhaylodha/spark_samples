package my.spark

import my.spark.common_utils.SparkRunner
import my.spark.common_utils.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, when, lit }

object NullCheck extends SparkRunner with Logger {
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val df = Seq(
      ("2011-12", 1),
      ("2012-13", 2),
      ("2013-14", 3),
      ("2014-15", 4),
      ("2015-16", 5),
      ("2016-17", 6),
      ("2017-18", 7),
      ("2018-19", 8))
      .toDF("duration", "id")
      .withColumn("volume1", when(col("id").between(3, 6), lit(null: String)).otherwise(col("id")))
      .withColumn("volume2", when(col("id").between(5, 8), lit(null: String)).otherwise(col("id")))
      .withColumn("condition_cols_with_null", col("volume1") === col("volume2"))
      .withColumn("condition_true_and_cols_with_null", (lit(true) && col("volume1") === col("volume2")))
      .withColumn("condition_true_or_cols_with_null", (lit(true) || col("volume1") === col("volume2")))

    //Full input data
    df.show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |2011-12 |1  |1      |1      |true                    |true                             |true                            |
     * |2012-13 |2  |2      |2      |true                    |true                             |true                            |
     * |2013-14 |3  |null   |3      |null                    |null                             |true                            |
     * |2014-15 |4  |null   |4      |null                    |null                             |true                            |
     * |2015-16 |5  |null   |null   |null                    |null                             |true                            |
     * |2016-17 |6  |null   |null   |null                    |null                             |true                            |
     * |2017-18 |7  |7      |null   |null                    |null                             |true                            |
     * |2018-19 |8  |8      |null   |null                    |null                             |true                            |
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     */

    //Null not equals to Null Comparison should have yield some output.
    df.filter(col("volume1") =!= col("volume2")).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     */

    //Null to Null Comparison does not yield any output.
    df.filter(col("volume1") === col("volume2")).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |2011-12 |1  |1      |1      |true                    |true                             |true                            |
     * |2012-13 |2  |2      |2      |true                    |true                             |true                            |
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     *
     */

    //Null to Null Comparison along with other conditions also does not yield any output.
    df.filter(lit(true) && col("volume1") === col("volume2")).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |2011-12 |1  |1      |1      |true                    |true                             |true                            |
     * |2012-13 |2  |2      |2      |true                    |true                             |true                            |
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     *
     */

    //comparing with null as === yields no result at all.
    df.filter(col("volume1") === null).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     *
     */

    //comparing with null as =!= yields no result at all.
    df.filter(col("volume1") =!= null).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     *
     */

    //comparing with null as isNull yields correct result.
    df.filter(col("volume1").isNull).show(false);
    /**
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |duration|id |volume1|volume2|condition_cols_with_null|condition_true_and_cols_with_null|condition_true_or_cols_with_null|
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     * |2013-14 |3  |null   |3      |null                    |null                             |true                            |
     * |2014-15 |4  |null   |4      |null                    |null                             |true                            |
     * |2015-16 |5  |null   |null   |null                    |null                             |true                            |
     * |2016-17 |6  |null   |null   |null                    |null                             |true                            |
     * +--------+---+-------+-------+------------------------+---------------------------------+--------------------------------+
     */
  }

}