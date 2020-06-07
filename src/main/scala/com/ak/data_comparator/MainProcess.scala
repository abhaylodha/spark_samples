package com.ak.data_comparator

import org.apache.spark.sql.SparkSession

import my.spark.common_utils.Logger
import my.spark.common_utils.SparkRunner
import com.ak.data_comparator.config.ConfigReader
import scala.reflect.io.File
import java.io.BufferedReader
import com.ak.data_comparator.utils.DataFrameUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ row_number, col }
import com.ak.data_comparator.utils.StorageUtils
import com.ak.excel_utils.ExcelUtils
import org.apache.spark.sql.DataFrame

object MainProcess extends SparkRunner with Logger {

  def run(spark: SparkSession, args: Array[String]): Unit = {
    val config = ConfigReader.getConfig

    val df1 =
      DataFrameUtils.getDFFromJarData(
        spark,
        getClass.getResource(config.source_1.file_path).getPath())

    val df2 =
      DataFrameUtils.getDFFromJarData(
        spark,
        getClass.getResource(config.source_2.file_path).getPath())

    val allDFs = DataFrameUtils.compareDataFrames(spark, config, df1, df2)
    implicit val storageContext = StorageUtils.StorageUtilsContext("my_schema", "my_table", "2020-06-06", "run_1", 50)

    val (genericDF1, genericQuery1) = StorageUtils.getGenericDataFrame(spark, allDFs._1, s"Only in ${config.source_1.name}")
    val (genericDF2, genericQuery2) = StorageUtils.getGenericDataFrame(spark, allDFs._2, s"Only in ${config.source_2.name}")
    val (genericDF3, genericQuery3) = StorageUtils.getGenericDataFrame(spark, allDFs._3, "Available in both")

    val sqlc = spark.sqlContext
    import sqlc.implicits._
    val matching = allDFs._3.filter(col("mismatch") === "")
    val mismatching = allDFs._3.filter(col("mismatch") =!= "")

    val summaryDF = Seq(
      ("2020-06-06", s"Only in ${config.source_1.name} Table", allDFs._1.count, genericQuery1),
      ("2020-06-06", s"Only in ${config.source_2.name} Table", allDFs._2.count, genericQuery2),
      ("2020-06-06", s"Common in both", allDFs._3.count, genericQuery3),
      ("2020-06-06", s"Matching in Common", matching.count, genericQuery3 + " where mismatch = ''"),
      ("2020-06-06", s"MisMatch in Common", mismatching.count, genericQuery3 + " where mismatch != ''")).toDF(
        "_bus_dt_", "particular", "value", "query")

    summaryDF.show(false)

    val xlsSvc = new ExcelUtils("/F:/Workspaces/PRJ_ComparatorDisplay/result_explorer/Data.xls")

    xlsSvc.insertAllRecords("summary", convertDFToList(summaryDF))
    xlsSvc.insertAllRecords("data", convertDFToList(genericDF1))
    xlsSvc.insertAllRecords("data", convertDFToList(genericDF2))
    xlsSvc.insertAllRecords("data", convertDFToList(genericDF3))

  }

  implicit def convertDFToList(df: DataFrame): java.util.List[java.util.List[String]] = {
    import scala.collection.JavaConverters._

    val noOfColumns = df.columns.length

    df.collect().map(r =>
      (0 to noOfColumns - 1).foldLeft(List[String]())((data, colNo) => data ++ Seq(
        if (r.get(colNo) != null)
          r.get(colNo).toString
        else
          ""))).toList
      .map(r => r.asJava).asJava
  }

}