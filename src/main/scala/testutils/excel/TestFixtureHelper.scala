package testutils.excel

import org.junit.runner.RunWith
import org.concordion.integration.junit4.ConcordionRunner
import org.concordion.api.extension.Extensions
import org.concordion.ext.EmbedExtension
import org.concordion.api.option.ConcordionOptions

import testutils.excel.Result
import testutils.excel.ExcelReader
import student_grades.mapper.MappingUtils
import testutils.excel.DFTestUtils.verifyDFContent
import my.spark.common_utils.SparkRunner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

trait TestFixtureHelper {

  var fName = ""
  var excelData: Map[String, (Seq[Seq[String]], DataFrame)] = Map[String, (Seq[Seq[String]], DataFrame)]()
  var excelDataDF: Map[String, DataFrame] = Map[String, DataFrame]()
  var excelDataRaw: Map[String, Seq[Seq[String]]] = Map[String, Seq[Seq[String]]]()

  def setFileName(excelPath: String) = {
    fName = excelPath
    excelData = readData(fName)
    excelDataDF = excelData.map(a => (a._1, a._2._2))
    excelDataRaw = excelData.map(a => (a._1, a._2._1))
  }

  def printData(_tName: String) = {

    val header = excelDataRaw(_tName).head.map(heading => s"<TH>$heading</TH>").mkString("")

    val t = s"<TABLE>$header<TR>" +
      excelDataRaw(_tName).tail.tail.map(row => row.map(cell => s"<TD>$cell</TD>").mkString("")).mkString("</TR><TR>") +
      "</TR></TABLE>"
    t
  }

  def readData(excelPath: String) = {
    class TestSession extends SparkRunner {
      def run(spark: SparkSession, strArray: Array[String]) = {}
    }

    ExcelReader
      .getDataFramesFromExcelWorkbook(
        excelPath,
        (new TestSession()).getSparkSession)
  }

}