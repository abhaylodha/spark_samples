package com.ak.data_comparator.utils

import my.spark.common_utils.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ lit }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder

object StorageUtils extends Logger {

  case class StorageUtilsContext(
    storageSchemaName: String,
    storageTableName: String,
    busDt: String,
    runId: String,
    numberOfColumnsInTable: Int)

  def getGenericDataFrame(spark: SparkSession, df: DataFrame,
    comment: String)(implicit storageUtilsContext: StorageUtilsContext) = {
    val stageId = StageId.getNextStageId

    val sqlc = spark.sqlContext
    import sqlc.implicits._

    val columns = df.columns.toSeq
    val runId = storageUtilsContext.runId
    val busDt = storageUtilsContext.busDt

    case class ColNameToHeader(genericName: String, actualName: String)

    val colNameToHeaderMap = columns.foldLeft(
      (1, Seq[ColNameToHeader]()))(
        (columnNoAndHeader, actualName) => (columnNoAndHeader._1 + 1,
          columnNoAndHeader._2 ++ Seq(ColNameToHeader("col" + columnNoAndHeader._1, actualName))))._2

    val dataDF = colNameToHeaderMap
      .foldLeft(df)((df, columnMapping) => {
        df.withColumnRenamed(columnMapping.genericName, columnMapping.actualName)
      })
      .withColumn("_id_", lit(s"${runId}_${stageId}_2"))
      .withColumn("_bus_dt_", lit(busDt))
      .withColumn("_comment_", lit(comment))

    val colNamesDF = colNameToHeaderMap
      .foldLeft(Seq[String](("")).toDF("dummy"))(
        (a, colNameToHeaderMapping) =>
          a.withColumn(
            colNameToHeaderMapping.genericName, lit(colNameToHeaderMapping.actualName))).drop("dummy")
      //      .withColumn("_id_", lit(s"${runId}_${stageId}_1"))
      //      .withColumn("_bus_dt_", lit(busDt))
      //      .withColumn("_comment_", lit(comment))
      //	Adjustment for excel data writing
      .withColumn("_id_", lit("_id_"))
      .withColumn("_bus_dt_", lit("_bus_dt_"))
      .withColumn("_comment_", lit("_comment_"))

    val finalDF = colNamesDF.union(dataDF)
    //    val finalDF = ((columns.length + 1 to
    //      storageUtilsContext.numberOfColumnsInTable)
    //      .foldLeft(colNamesDF.union(dataDF))(
    //        (df, colNo) => df.withColumn(s"col$colNo", lit(null))))

    val additionalColumns = (columns.length + 1 to
      storageUtilsContext.numberOfColumnsInTable)
      .foldLeft(Seq[String]())((data, colNo) => data ++ Seq(s"col$colNo"))
      .map(StructField(_, StringType))

    val newSchema = StructType(finalDF.schema.fields ++ additionalColumns)

    finalDF.mapPartitions(iterator => {
      iterator.map(item => {
        val nullColumns = (columns.length + 1 to
          storageUtilsContext.numberOfColumnsInTable)
          .foldLeft(Seq[String]())((data, colNo) => data ++ Seq(null: String))
        Row.fromSeq(item.toSeq ++ nullColumns)
      })
    })(RowEncoder(newSchema))

    //finalDF.show(1000, false)

    val query_1 = colNameToHeaderMap.foldLeft(s"with `id_${runId}_${stageId}` as \n(select \n")((
      qry, colNameToHeaderMap) => qry + "  `" + colNameToHeaderMap.genericName +
      "` as `" + colNameToHeaderMap.actualName + "`,\n")
    val query_2 = query_1.substring(0, query_1.length - 2)
    val query_3 = query_2 +
      s"\nfrom `${storageUtilsContext.storageSchemaName}`.`${storageUtilsContext.storageTableName}`\nwhere " +
      s"`_id_` = '${runId}_${stageId}_2' and `_bus_dt_` = '${busDt}')\nselect * from `id_${runId}_${stageId}`"

    info(s"Data Query : $query_3")
    (finalDF, query_3)
  }

}