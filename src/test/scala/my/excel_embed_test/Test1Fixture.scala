package my.excel_embed_test

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
import testutils.excel.TestFixtureHelper

@ConcordionOptions(declareNamespaces = { Array("cx", "urn:concordion-extensions:2010") })
@Extensions(Array(classOf[EmbedExtension]))
@RunWith(classOf[ConcordionRunner])
class Test1Fixture extends TestFixtureHelper {

  def runTest1(excelPath: String): Result = {

    //val excelData = readData(excelPath)

    val joinedResult = MappingUtils
      .joinStudentAndGrades(excelDataDF("my.student"), excelDataDF("my.grades"))

    verifyDFContent(joinedResult, excelDataDF("verify"))
  }

}