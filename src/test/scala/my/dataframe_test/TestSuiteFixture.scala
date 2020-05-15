package my.dataframe_test

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith

import my.spark.common_utils.SparkRunner
import student_grades.mapper.MappingUtils
import testutils.excel.DFTestUtils.verifyDFContent
import testutils.excel.ExcelReader
import testutils.excel.Result

import org.concordion.api.extension.Extensions;
import org.concordion.api.option.ConcordionOptions;
import org.concordion.integration.junit4.ConcordionRunner
import org.concordion.ext.EmbedExtension

@ConcordionOptions(declareNamespaces = { Array("cx", "urn:concordion-extensions:2010") })
@Extensions(Array(classOf[EmbedExtension]))
@RunWith(classOf[ConcordionRunner])
class TestSuite {

  def runTest1(excelPath: String): Result = {

    val excelData = readDataFrames(excelPath)

    val joinedResult = MappingUtils
      .joinStudentAndGrades(excelData("my.student")._2, excelData("my.grades")._2)

    verifyDFContent(joinedResult, excelData("verify")._2)
  }

  def getData1() = {
    "<Strong>Abhay</Strong>"
  }

  def runTest2(excelPath: String): Result = {

    val excelData = readDataFrames(excelPath)

    val aboveNinety = MappingUtils.selectOnlyAboveNinety(excelData("my.student")._2)

    aboveNinety.show(false)
    excelData("verify")._2.show(false)

    verifyDFContent(aboveNinety, excelData("verify")._2)
  }

  def readDataFrames(excelPath: String) = {
    class TestSession extends SparkRunner {
      def run(spark: SparkSession, strArray: Array[String]) = {}
    }

    ExcelReader
      .getDataFramesFromExcelWorkbook(
        excelPath,
        (new TestSession()).getSparkSession)
  }

}
