package testutils.excel

import java.io.InputStream

import scala.collection.JavaConversions.seqAsJavaList

import org.apache.poi.hssf.usermodel.HSSFCell
import org.apache.poi.hssf.usermodel.HSSFRow
import org.apache.poi.hssf.usermodel.HSSFSheet
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.poifs.filesystem.POIFSFileSystem
import org.apache.poi.ss.usermodel.CellType.{
  BLANK,
  BOOLEAN,
  ERROR,
  FORMULA,
  NUMERIC,
  STRING
}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

import my.spark.common_utils.Logger

object ExcelReader extends Logger {
  def getDataFramesFromExcelWorkbook(path: String, spark: SparkSession): Map[String, (Seq[Seq[String]], DataFrame)] = {

    info(s"Reading :- $path")

    val fileIn: InputStream = getClass.getResourceAsStream(path)
    val fs: POIFSFileSystem = new POIFSFileSystem(fileIn);
    val wb: HSSFWorkbook = new HSSFWorkbook(fs);

    val workbookValue = for (sheetNo <- 0 to wb.getNumberOfSheets() - 1) yield {
      val sheet: HSSFSheet = wb.getSheetAt(sheetNo)
      val sheetValue = for (rowNo <- 0 to sheet.getPhysicalNumberOfRows() - 1) yield {
        val row: HSSFRow = sheet.getRow(rowNo)
        val rowValue = for (cellNo <- 0 to row.getPhysicalNumberOfCells - 1) yield {
          val cell: HSSFCell = row.getCell(cellNo)
          if (cell != null) {

            val t = cell.getCellType() match {
              case FORMULA => {
                cell.getCellFormula()
              }
              case NUMERIC => {
                cell.getNumericCellValue().toString
              }
              case STRING => {
                cell.getStringCellValue
              }
              case BLANK => {
                "<BLANK>"
              }
              case BOOLEAN => {
                cell.getBooleanCellValue.toString
              }
              case ERROR => {
                cell.getErrorCellValue.toString
              }
              case _ => {
                "UNKNOWN"
              }
            }
            t
          } else {
            "NULL"
          }
        }
        rowValue
      }
      (sheet.getSheetName -> (sheetValue, getDataFrame(sheet, sheetValue, spark)))
    }
    workbookValue.toMap
  }

  def getDataFrame(sheet: HSSFSheet, sheetValue: Seq[Seq[String]], spark: SparkSession) = {

    val sheetValueNew = if (sheet.getSheetName == "execute") {
      Seq(Seq("action")) ++ Seq(Seq("string")) ++ sheetValue
    } else {
      sheetValue
    }

    val colNames = sheetValueNew.head
    val dataType = sheetValueNew.tail.head

    val columns = colNames.zip(dataType)

    val data = sheetValueNew.tail.tail.map(d => Row.fromSeq(d))

    columns.foldLeft(
      spark.createDataFrame(
        seqAsJavaList(data),
        StructType(
          columns.map(d => StructField(d._1, StringType, true)))))(
        (df, column) => df.withColumn(column._1, col(column._1).cast(column._2)))
  }
}