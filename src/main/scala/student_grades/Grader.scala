package student_grades

import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, lit }
import org.apache.spark.sql.DataFrame
import testutils.excel.ExcelReader
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import student_grades.mapper.MappingUtils

object Grader extends SparkRunner {

  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val excelData = ExcelReader
      .getDataFramesFromExcelWorkbook("/my/input_data/Input_Data.xls", spark)
    info("excelData + " + excelData)

    MappingUtils.joinStudentAndGrades(excelData("my.student")._2, excelData("my.grades")._2)
      .show(false);
  }

}
