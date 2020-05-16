package student_grades.mapper

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,lit}

object MappingUtils {
  def joinStudentAndGrades(studentDF: DataFrame, gradeDF: DataFrame): DataFrame = {

    studentDF.join(
      gradeDF,
      col("percentage").between(col("percentage_from"), col("percentage_to")))

  }

  def selectOnlyAboveNinety(studentDF: DataFrame): DataFrame = {

    studentDF.filter(col("percentage") > 90)

  }
}