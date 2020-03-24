package org.sergiocc.tmdb.util

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.sergiocc.tmdb.config.GenreRatingJobParameters

trait Persistence extends AppLogger {

  private val COMMA = ","


  /**
   * Register in Hive partitions which don't exist.
   * @param spark         Spark session to use.
   * @param df            Data frame to get different values.
   * @param partitionCol  Partition col defined.
   * @param params        Parameters parsed.
   */
  private def registerPartitions(spark: SparkSession, df: DataFrame, partitionCol: String,
                                 params: GenreRatingJobParameters): Unit = {
    val sqlList = df.select(partitionCol)
      .distinct
      .collect
      .map(r => r.getInt(0))

    sqlList.foreach(date => {
      val location = params.outputTable + "/" + partitionCol + "=" + date + "/"
      val sql = s"ALTER TABLE ${params.outputTableName} ADD IF NOT EXISTS PARTITION " +
        s"($partitionCol=$date) " +
        s"LOCATION '$location'"
      log.info("SQL -> " + sql)
      spark.sql(sql)
    })
  }


  /**
   * Returns column list in a String with data type.
   * @param  df           Data frame to extract columns.
   * @param  partCols     Partition columns, by default excluded.
   * @param  getPartCols  Flag for getting only partition columns
   * @return              Column list in a String with data type.
   */
  private def getColumnsFromDF(df: DataFrame, partCols: String, getPartCols: Boolean): String = {
    val partColsTrim = COMMA + partCols.replace(" ", "").trim + COMMA

    df.schema
      .fields
      .filter(field => {
        if (getPartCols) {
          partColsTrim.contains(COMMA + field.name + COMMA)
        } else {
          !partColsTrim.contains(COMMA + field.name + COMMA)
        }
      })
      .map(_.toDDL)
      .reduce( (f1, f2) => f1 + COMMA + f2)
  }


  /**
   * Create output table if not exists.
   * @param spark         Spark session to use.
   * @param params        Parameters parsed supplied by command line.
   * @param df            Data frame to use as reference.
   * @param partitionCol  Column list for defining partitions, data type included.
   */
  private def registerTableHive(spark: SparkSession, params: GenreRatingJobParameters,
                                partitionCol: String, df: DataFrame): Unit = {
    val columnList = getColumnsFromDF(df, partitionCol, getPartCols = false)
    val partColumnList = getColumnsFromDF(df, partitionCol, getPartCols = true)
    val sql =
      s"""
        | CREATE EXTERNAL TABLE IF NOT EXISTS ${params.outputTableName} ( $columnList )
        | PARTITIONED BY ($partColumnList)
        | STORED AS PARQUET LOCATION '${params.outputTable}'
        |""".stripMargin
    log.info("SQL -> " + sql)
    spark.sql(sql)
  }


  /**
   * Method which writes data on disk.
   * @param spark         Spark session to use.
   * @param partitionCol  Column to partition data.
   * @param fileParts     Number of file parts inside the partition.
   * @param params        Parameters parsed supplied by command line.
   * @param df            Data frame to write.
   */
  def writeToDatabase(spark: SparkSession, partitionCol: String, fileParts: Int,
                      params: GenreRatingJobParameters, df: DataFrame): Unit = {
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df.repartition(fileParts)
      .write
      .partitionBy(partitionCol)
      .mode(SaveMode.Overwrite)
      .parquet(params.outputTable)

    registerTableHive(spark, params, partitionCol, df)
    registerPartitions(spark, df, partitionCol, params)
  }

}
