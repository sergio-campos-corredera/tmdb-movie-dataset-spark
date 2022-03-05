package org.sergiocc.tmdb.util

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder {

  private val TRUE = "true"


  /**
   * Method which creates a spark session.
   * @param appName App name to use in session.
   * @return        A spark session.
   */
  def getOrCreate(appName: String): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .config("hive.cbo.enable", TRUE)
      .config("hive.compute.query.using.stats", TRUE)
      .config("hive.exec.parallel", TRUE)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.stats.fetch.column.stats", TRUE)
      .config("hive.stats.fetch.partition.stats", TRUE)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.writeLegacyFormat", TRUE)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.locality.wait", "0")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .getOrCreate()

    session.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", TRUE)
    session
  }


  /**
   * Method which creates a spark session for testing.
   * @param appName App name to use in session.
   * @return        A spark session for testing.
   */
  def getOrCreateForTest(appName: String): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .enableHiveSupport()
      .config("hive.cbo.enable", TRUE)
      .config("hive.compute.query.using.stats", TRUE)
      .config("hive.exec.parallel", TRUE)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.stats.fetch.column.stats", TRUE)
      .config("hive.stats.fetch.partition.stats", TRUE)
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.writeLegacyFormat", TRUE)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.locality.wait", "0")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
      .master("local[*]")
      .getOrCreate()

    session.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", TRUE)
    session
  }
}
