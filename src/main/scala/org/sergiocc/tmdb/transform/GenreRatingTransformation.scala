package org.sergiocc.tmdb.transform

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.sergiocc.tmdb.config.GenreRatingJobParameters
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, lit, max, regexp_replace, sequence, sum, udf, when}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType}
import org.json.simple.{JSONArray, JSONObject}
import org.json.simple.parser.JSONParser


trait GenreRatingTransformation {

  private val COL_GENRES = "genres"
  private val COL_RELEASE_DATE = "release_date"
  private val COL_MAX_DATE = "max_date"
  private val COL_DATE = "date"
  private val COL_GENRE_ID = "genre_id"
  private val COL_GENRE_NAME = "genre_name"
  private val COL_VOTE_COUNT = "vote_count"
  private val COL_VOTE_AVERAGE = "vote_average"
  private val COL_WEIGHT = "weight"
  
  private val CHAR_DOUBLE_QUOTE = "\""

  case class GenresArray(genresList: List[(Long, String)])

  /*
   * UDF Functions
   */

  /**
   * UDF function which converts a String which represent a JSON Array into a Array[String]
   * @return A Array[String] with JSON objects inside.
   */
  val toArrayUDF: UserDefinedFunction = udf(
    f = (value: String) => {
      val parser = new JSONParser()
      parser.parse(value)
        .asInstanceOf[JSONArray]
        .toArray
        .map(obj => obj.asInstanceOf[JSONObject])
        .map(obj => (obj.get("id").asInstanceOf[Long], obj.get("name").toString))
    }
  )


  /*
   * Public methods
   */

  /**
   * Method which read input data set.
   * @param spark   Spark session to use.
   * @param params  Genre rating parameters supplied.
   * @return        A data frame with input data.
   */
  def trfCreateDataFrame(spark: SparkSession, params: GenreRatingJobParameters) : DataFrame = {
    spark.read
      .option("header", "true")
      .option("quote", CHAR_DOUBLE_QUOTE)
      .option("escape", CHAR_DOUBLE_QUOTE)
      .csv(params.inputTable)
  }


  /**
   * Method which transform data flattening genre (generating one row per genre).
   * @param spark   Spark session to use.
   * @param df      Data frame with input data.
   * @return        A flattened data frame by genre.
   */
  def trfFlatteningGenre(spark: SparkSession) (df: DataFrame): DataFrame = {
    df.select(COL_GENRES, COL_RELEASE_DATE, COL_VOTE_AVERAGE, COL_VOTE_COUNT)
      .withColumn(COL_GENRES, when(col(COL_GENRES).like("%id%"), toArrayUDF(col(COL_GENRES))) )
      .withColumn(COL_GENRES, explode(col(COL_GENRES)))
      .withColumn(COL_GENRE_ID, col("genres._1").cast(IntegerType))
      .withColumn(COL_GENRE_NAME, col("genres._2"))
      .drop(COL_GENRES)
  }


  /**
   * Method which fills missing day to create a complete histogram.
   * @param spark   Spark session to use.
   * @param df      Data frame with genre flattened data.
   * @return        A day filled data frame.
   */
  def trfFillMissingDays(spark: SparkSession) (df: DataFrame): DataFrame = {
    val maxDate = df.agg(max(COL_RELEASE_DATE).as(COL_RELEASE_DATE))
        .head
        .getString(0)

    df.withColumn(COL_RELEASE_DATE, col(COL_RELEASE_DATE).cast(DateType))
      .withColumn(COL_MAX_DATE, lit(maxDate).cast(DateType))
      .withColumn(COL_DATE, explode(sequence(col(COL_RELEASE_DATE), col(COL_MAX_DATE))))
      .drop(COL_RELEASE_DATE)
      .drop(COL_MAX_DATE)
      .withColumn(COL_DATE, regexp_replace(col(COL_DATE).cast(StringType), "-", "").cast(IntegerType))
  }


  /**
   * Method which aggregates data per genre, calculating an average rating.
   * @param spark   Spark session to use.
   * @param df      Data frame with filled days.
   * @return        A aggregated data frame per genre and day with rating average.
   */
  def trfAggregatingGenre(spark: SparkSession) (df: DataFrame): DataFrame = {
    df.withColumn(COL_WEIGHT, sum(COL_VOTE_COUNT) over Window.partitionBy(COL_DATE, COL_GENRE_ID, COL_GENRE_NAME))
      .withColumn(COL_WEIGHT, col(COL_VOTE_COUNT) / col(COL_WEIGHT))
      .withColumn(COL_VOTE_AVERAGE, col(COL_VOTE_AVERAGE) * col(COL_WEIGHT))
      .groupBy(COL_DATE, COL_GENRE_ID, COL_GENRE_NAME)
      .agg(sum(COL_VOTE_COUNT).as(COL_VOTE_COUNT),
        sum(COL_VOTE_AVERAGE).as(COL_VOTE_AVERAGE))
  }

}
