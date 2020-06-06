package com.ak.data_comparator.utils

import org.apache.spark.sql.SparkSession
import my.spark.common_utils.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{ col, lit, substring }
import com.ak.data_comparator.config.Config
import org.apache.spark.sql.catalyst.expressions.Encode
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row

case class ColumnNameMapping(origName: String, table1ColName: String, table2ColName: String)

object DataFrameUtils extends Logger {
  def getDFFromJarData(spark: SparkSession, path: String) = {

    info(s"Reading $path")

    spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
  }

  def compareDataFrames(spark: SparkSession, config: Config, _df1: DataFrame, _df2: DataFrame) = {
    val requiredColumns = (config.comparator.join_columns_as_seq ++
      config.comparator.columns_to_compare_as_seq)
    val columnsMapping = getColumnMapping(config)

    val joinCondition = getCondition(config)

    val df1 = _df1.select(requiredColumns.map(col): _*).withColumn("X_X_X_CHECK_COLUMN_X_X_X", lit("1"))
      .transform(renameColumns(config.source_1.name, requiredColumns))
    val df2 = _df2.select(requiredColumns.map(col): _*).withColumn("X_X_X_CHECK_COLUMN_X_X_X", lit("1"))
      .transform(renameColumns(config.source_2.name, requiredColumns))

    val leftJoinDF = df1.join(df2, joinCondition, "left_outer")
      .filter(col(s"${config.source_2.name}_X_X_X_CHECK_COLUMN_X_X_X").isNull)
    val rightJoinDF = df1.join(df2, joinCondition, "right_outer")
      .filter(col(s"${config.source_1.name}_X_X_X_CHECK_COLUMN_X_X_X").isNull)
    val innerJoinDF = df1.join(df2, joinCondition)
      .transform(compareRecords(spark, columnsMapping))

    val onlyInLeft = leftJoinDF.count
    val onlyInRight = rightJoinDF.count
    val inBoth = innerJoinDF.count

    info(s"Only in ${config.source_1.name} Table : $onlyInLeft")
    info(s"Only in ${config.source_2.name} Table : $onlyInRight")
    info(s"Common in both : $inBoth")
    val comparedDF = formatData(innerJoinDF, config, columnsMapping)

    (leftJoinDF, rightJoinDF, comparedDF)
  }

  def renameColumns(prefix: String, requiredColumns: Seq[String])(df: DataFrame): DataFrame = {

    (requiredColumns ++ Seq("X_X_X_CHECK_COLUMN_X_X_X")).foldLeft(df)((df, colName) => {
      df.withColumnRenamed(colName, s"${prefix}_${colName}")
    })
  }

  def getColumnMapping(config: Config) = {

    val requireColumns = config.comparator.join_columns_as_seq ++ config.comparator.columns_to_compare_as_seq

    requireColumns.foldLeft(Seq[ColumnNameMapping]())((pair, colName) => {
      pair ++ Seq(ColumnNameMapping(colName, s"${config.source_1.name}_${colName}", s"${config.source_2.name}_${colName}"))
    })

  }

  def getCondition(config: Config) = {
    config.comparator.join_columns_as_seq.foldLeft(lit(true))((cond, colName) => {
      cond && (col(s"${config.source_1.name}_${colName}") === col(s"${config.source_2.name}_${colName}"))
    })
  }

  def formatData(outputDF: DataFrame, config: Config, columnMapping: Seq[ColumnNameMapping]): DataFrame = {

    val orderCols = config.comparator.join_columns_as_seq.map(col) ++ Seq(col("_source_table_"))
    val part1Cols = columnMapping.map(i => col(i.table1ColName).as(i.origName)) ++ Seq(col("mismatch"))
    val part2Cols = columnMapping.map(i => col(i.table2ColName).as(i.origName)) ++ Seq(col("mismatch"))
    val part1DF = outputDF.select(part1Cols: _*).withColumn("_source_table_", lit(s"1-${config.source_1.name}"))
    val part2DF = outputDF.select(part2Cols: _*).withColumn("_source_table_", lit(s"2-${config.source_2.name}"))

    val finalColumns = (config.comparator.join_columns_as_seq ++
      Seq("_source_table_", "mismatch") ++ config.comparator.columns_to_compare_as_seq).map(col)

    val data = part1DF.union(part2DF)
      .orderBy(orderCols: _*)
      .withColumn("_source_table_", substring(col("_source_table_"), 3, 100))
      .select(finalColumns: _*)

    //data.show(false)

    data
  }

  def compareRecords(spark: SparkSession, columnsMapping: Seq[ColumnNameMapping])(df: DataFrame): DataFrame = {

    val columnsMappingAsBroadcast = spark.sparkContext.broadcast(columnsMapping)
    val currentColumnSequenceAsBroadcast = spark.sparkContext.broadcast(df.schema.fields.map(_.name))

    val sch = StructType(
      df.schema.fields
        ++ Seq(StructField("mismatch", StringType)))

    df.map(row => {
      //      println("columnsMappingAsBroadcast : ")
      //      println(columnsMappingAsBroadcast.value.map(_.origName).mkString(","))
      //      println("currentColumnSequenceAsBroadcast : ")
      //      println(currentColumnSequenceAsBroadcast.value.mkString(","))

      val matchResult = columnsMappingAsBroadcast.value.foldLeft("")((result, columnMap) => {
        val value1 = row.getAs[Any](columnMap.table1ColName)
        val value2 = row.getAs[Any](columnMap.table2ColName)
        if (value1 == value2)
          result
        else
          result + "," + columnMap.origName
      })

      val newRow = currentColumnSequenceAsBroadcast.value.foldLeft((Seq[Any](), 0))((values, colName) => {
        (values._1 ++ Seq(row.get(values._2)), values._2 + 1)
      })

      Row.fromSeq(newRow._1 ++ Seq(matchResult.stripPrefix(",")))
    }, RowEncoder(sch))

  }

}

  //    info("Cleaned")
  //    df1.withColumn("rn", row_number.over(Window.partitionBy("name", "year")
  //      .orderBy(col("acceleration"))))
  //      .filter(col("rn") === 1)
  //      .repartition(1)
  //      .write
  //      .format("csv")
  //      .option("header", "true")
  //      .mode("overwrite")
  //      .save("/F:/Workspaces/Scala/spark_samples/src/src/main/resources/com/ak/data_comparator/data/cars_new")
