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

    StorageUtils.persistDataFrame(spark, allDFs._1, s"Only in ${config.source_1.name}")
    StorageUtils.persistDataFrame(spark, allDFs._2, s"Only in ${config.source_2.name}")
    StorageUtils.persistDataFrame(spark, allDFs._3, "Available in both")

  }

}