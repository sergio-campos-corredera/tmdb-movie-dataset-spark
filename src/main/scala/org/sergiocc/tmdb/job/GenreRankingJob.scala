package org.sergiocc.tmdb.job

import org.sergiocc.tmdb.config.{GenreRatingJobParameters, GenreRatingParameters}
import org.sergiocc.tmdb.transform.GenreRatingTransformation
import org.sergiocc.tmdb.util.{AppLogger, Persistence, SparkSessionBuilder}
import org.apache.spark.sql.SparkSession

object GenreRankingJob extends GenreRatingParameters
  with GenreRatingTransformation
  with AppLogger
  with Persistence {

  /**
   * Method which build params.
   * @param args  Parameters supplied by command line.
   * @return      Parameters in a GereRatingJobParameters object.
   */
  def buildParams(args: Seq[String]): GenreRatingJobParameters = {
    buildJobParams(args)
  }


  /**
   * Method with contains transformations flow.
   * @param spark   Spark session to use.
   * @param params  Genre rating parameters supplied.
   */
  def run(spark: SparkSession, params: GenreRatingJobParameters): Unit = {
    val df = trfCreateDataFrame(spark, params)
      .transform(trfFlatteningGenre(spark))
      .transform(trfFillMissingDays(spark))
      .transform(trfAggregatingGenre(spark))
      .transform(trfAddYear(spark))

    writeToDatabase(spark, COL_YEAR, 1, params, df)
  }


  /**
   * Main entry point for execution.
   * @param args  Parameters supplied by command line.
   */
  def main(args: Array[String]): Unit = {
    val config = buildParams(args)
    val appName = this.getClass.getName.stripSuffix("$")
    val spark = SparkSessionBuilder.getOrCreate(appName)
    run(spark, config)
  }

}

