package org.sergiocc.tmdb.config


case class GenreRatingJobParameters(inputTable: String = "",
                                    outputTable: String = "",
                                    outputTableName: String = "")


trait GenreRatingParameters {

  private val INPUT_TABLE = "input-table"
  private val OUTPUT_TABLE = "output-table"
  private val OUTPUT_TABLE_NAME = "output-table-name"


  /**
   * Method which parses parameters in a GenreRatingJobParameters object.
   * @param args  Arguments supplied by command line.
   * @return      A GenreRatingJobParameters object with paremeters parsed.
   */
  def buildJobParams(args: Seq[String]): GenreRatingJobParameters = {

    val parser = new scopt.OptionParser[GenreRatingJobParameters](this.getClass.getName) {

      opt[String](INPUT_TABLE)
        .required()
        .action((value, c) => c.copy(inputTable = value))
        .text("Table input path to use as source.")

      opt[String](OUTPUT_TABLE)
        .required()
        .action((value, c) => c.copy(outputTable = value))
        .text("Table output path to store results.")

      opt[String](OUTPUT_TABLE_NAME)
        .required()
        .action((value, c) => c.copy(outputTableName = value))
        .text(s"Table name which contains output data.")
    }

    val ret = parser.parse(args, GenreRatingJobParameters()) match {
      case Some(params) =>
        params
      case None =>
        GenreRatingJobParameters()
    }
    ret
  }

}
