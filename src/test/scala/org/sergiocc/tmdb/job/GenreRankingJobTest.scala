package org.sergiocc.tmdb.job

import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite._
import org.sergiocc.tmdb.util.{AppLogger, SparkSessionBuilder}

import scala.reflect.io.Directory

class GenreRankingJobTest extends AnyFunSuite
  with BeforeAndAfter
  with AppLogger {

  private var spark: SparkSession = _

  private val PRM_INPUT_TABLE = "/fixtures/tmdb_5000_movies_sample.csv"
  private val PRM_OUTPUT_TABLE = "/tmp/tmdb-test/outputData/tbl-genre-ranking"
  private val PRM_OUTPUT_TABLE_NAME = "tbl_genre_ranking"
  private val EXPECTED_OUTPUT = "/fixtures/expected_tbl_genre_ranking.json"

  private val COL_DATE = "date"
  private val COL_GENRE_ID = "genre_id"
  private val COL_GENRE_NAME = "genre_name"
  private val COL_VOTE_COUNT = "vote_count"
  private val COL_VOTE_AVERAGE = "vote_average"

  private val COL_R_DATE = "r_date"
  private val COL_R_GENRE_ID = "r_genre_id"
  private val COL_R_GENRE_NAME = "r_genre_name"
  private val COL_R_VOTE_COUNT = "r_vote_count"
  private val COL_R_VOTE_AVERAGE = "r_vote_average"


  /**
   * Clean possible outputs generated.
   * @param spark Spark session to use.
   */
  private def clean(spark: SparkSession): Unit = {
    val directory = new Directory(new File(PRM_OUTPUT_TABLE))
    directory.deleteRecursively()

    spark.sql(s"DROP TABLE IF EXISTS $PRM_OUTPUT_TABLE_NAME")
  }


  /**
   * Actions to do before each test.
   */
  before {
    spark = SparkSessionBuilder.getOrCreateForTest(this.getClass.getName.stripSuffix("$"))
    clean(spark)
  }


  /**
   * Test 1: Full job load.
   */
  test ("Genre ranking job test") {
    val inputFile = getClass.getResource(PRM_INPUT_TABLE).getPath
    val args = s"--input-table $inputFile --output-table $PRM_OUTPUT_TABLE --output-table-name $PRM_OUTPUT_TABLE_NAME"
      .split(" ")
    val params = GenreRankingJob.buildParams(args)

    GenreRankingJob.run(spark, params)

    val dfOutput = spark.read.table(PRM_OUTPUT_TABLE_NAME)

    val dfExpectedOutput = spark.read.json(getClass.getResource(EXPECTED_OUTPUT).getPath)
      .withColumnRenamed(COL_GENRE_ID, COL_R_GENRE_ID)
      .withColumnRenamed(COL_GENRE_NAME, COL_R_GENRE_NAME)
      .withColumnRenamed(COL_VOTE_COUNT, COL_R_VOTE_COUNT)
      .withColumnRenamed(COL_VOTE_AVERAGE, COL_R_VOTE_AVERAGE)
      .withColumnRenamed(COL_DATE, COL_R_DATE)

    val dfJoined = dfOutput.join(dfExpectedOutput,
      dfOutput(COL_GENRE_ID) === dfExpectedOutput(COL_R_GENRE_ID) &&
        dfOutput(COL_GENRE_NAME) === dfExpectedOutput(COL_R_GENRE_NAME) &&
        dfOutput(COL_VOTE_COUNT) === dfExpectedOutput(COL_R_VOTE_COUNT) &&
        dfOutput(COL_VOTE_AVERAGE) === dfExpectedOutput(COL_R_VOTE_AVERAGE) &&
        dfOutput(COL_DATE) === dfExpectedOutput(COL_R_DATE),
    "full_outer")

    assert(dfJoined.select(COL_GENRE_ID).filter(col(COL_GENRE_ID).isNull).count == 0 &&
      dfJoined.select(COL_R_GENRE_ID).filter(col(COL_R_GENRE_ID).isNull).count == 0)
  }

}
