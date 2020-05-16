package excelutils.charts

import my.spark.common_utils.SparkRunner
import java.util.ArrayList
import org.apache.spark.sql.SparkSession
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import chart_creator._
import chart_creator.ApachePoi
import scala.collection.immutable.List
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import scala.collection.JavaConverters._
import java.io.FileOutputStream
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.Row

object Chart1 extends SparkRunner {

  @Override
  def run(spark: SparkSession, args: Array[String]): Unit = {

    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val df = Seq(
      ("bmet.fidordermkt", "2020-01-01", 7745D),
      ("bmet.fidordermkt", "2020-01-02", 7657D),
      ("bmet.fidordermkt", "2020-01-03", 7457D),
      ("bmet.fidordermkt", "2020-01-04", 7675D),
      ("bmet.fidordermkt", "2020-01-05", 3677D),
      ("bmet.fidordermkt", "2020-01-06", 7437D),
      ("bmet.fidordermkt", "2020-01-07", 5477D),
      ("bmet.fidordermkt", "2020-01-08", 7574D),
      ("bmet.fidordermkt", "2020-01-09", 3747D),
      ("bmet.fidordermkt", "2020-01-10", 5777D),
      ("bmet.fidordermkt", "2020-01-11", 4797D),
      ("bmet.fidorderfill", "2020-01-01", 7745D),
      ("bmet.fidorderfill", "2020-01-02", 7657D),
      ("bmet.fidorderfill", "2020-01-03", 7457D),
      ("bmet.fidorderfill", "2020-01-04", 7675D),
      ("bmet.fidorderfill", "2020-01-05", 3677D),
      ("bmet.fidorderfill", "2020-01-06", 7437D),
      ("bmet.fidorderfill", "2020-01-07", 5477D),
      ("bmet.fidorderfill", "2020-01-08", 7574D),
      ("bmet.fidorderfill", "2020-01-09", 3747D),
      ("bmet.fidorderfill", "2020-01-10", 5777D),
      ("bmet.fidorderfill", "2020-01-11", 4797D)).toDF("table_name", "date", "count")

    val dfCollected = df.collect

    val dates = dfCollected.map(_.getAs[String]("date")).distinct
    val tableNames = dfCollected.map(_.getAs[String]("table_name")).distinct
    val wb = new XSSFWorkbook()

    tableNames.map(
      tableName => {
        val d1 = getFormattedData(dfCollected, tableName, dates)
        ApachePoi.barColumnChart(tableName, s"$tableName Data", "Dates", "Count", "Count", d1, wb)
      })

    // Write output to an excel file
    val filename = "DataAnalysis.xlsx"
    val fileOut = new FileOutputStream(filename)
    wb.write(fileOut)

  }

  def getFormattedData(_data: Array[Row], tableName: String, dates: Seq[String]): java.util.List[java.util.List[Object]] = {

    val data = _data.filter(r => r.getAs[String]("table_name") == tableName)
      .map(r => (r.getAs[String]("date"), r.getAs[Double]("count")))

    val d = dates.map(date => {
      data.filter(_._1 == date).headOption.getOrElse(date, 0D)
    })

    seqAsJavaList(Seq(
      seqAsJavaList(d.map(_._1)),
      seqAsJavaList(d.map(_._2).map(a => Double.box(a.toDouble)))))
  }

}
