package testutils.excel

import java.io.ByteArrayOutputStream

import org.apache.spark.sql.DataFrame

object DFTestUtils {
  def verifyDFContent(actualDF: DataFrame, expectedDF: DataFrame) = {
    val columnsInActualDF = actualDF.schema.fields.map(_.name)
    val columnsInExpectedDF = expectedDF.schema.fields.map(_.name)

    if ((columnsInActualDF ++ columnsInExpectedDF).distinct.size == columnsInActualDF.size) {
      val extraInActualDF = actualDF.except(expectedDF)
      val extraInExpectedDF = expectedDF.except(actualDF)

      if (extraInActualDF.count() > 0 || extraInExpectedDF.count > 0) {
        Result("failed", "Extra in actual resultset :<br/>    " + extraInActualDF.collect().mkString("<br/>    ")
          + "<br/>Extra in expected resultset :<br/>    " + extraInExpectedDF.collect().mkString("<br/>    "))
      } else {
        Result("passed", "All OK")
      }
    } else {
      Result("failed", "Columns differ in both Dataframes.<br/>Columns in actual resultset : " + columnsInActualDF.mkString("<br/>")
        + "<br/>Columns in expected resultset : " + columnsInExpectedDF.mkString("<br/>"))
    }

  }

  def captureOutput(df: DataFrame) = {
    print(df)
    val outCapture = new ByteArrayOutputStream
    Console.withOut(outCapture) {
      df.show(false)
    }
    val result = new String(outCapture.toByteArray)

    print(result)

    result
  }
}