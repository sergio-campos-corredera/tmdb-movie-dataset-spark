# The movie dataset - Spark processing

A sample spark project for processing The Movie Dataset. 

---

Last version generated: 2.0.0

---

## Prerequisites

 - Java 11
 - Scala 2.12
 - SBT 1.6.2
 - Spark 3.1.3

## SBT Lifecycle

### Compile

```bash
sbt clean compile
```

### Test

```bash
sbt clean test
```

### Package generation

```bash
sbt clean assembly
```

Output package located at: `./target/scala-2.12/tmdb-movie-dataset-spark-assembly-2.0.0.jar`

### Scoverage

```bash
sbt clean coverage test
sbt coverageReport
```

Output report located at: `./target/scala-2.12/scoverage-report/index.html` 

### Scala Style

```bash
sbt scalastyle
```

Output report located at: `./target/scalastyle-result.xml`

## Input data

Input data has been taken from Kaggle datasets. For getting more info about it visit URL 
https://www.kaggle.com/tmdb/tmdb-movie-metadata#tmdb_5000_movies.csv


## Run

```bash
spark-submit --class org.sergiocc.tmdb.job.GenreRankingJob ./target/scala-2.12/tmdb-movie-dataset-spark-assembly-2.0.0.jar \
             --input-table ./inputData/tmdb_5000_movies.csv \
             --output-table ./target/tmdb/tbl-genre-ranking \
             --output-table-name tbl_genre_ranking
```

## Output model

### Genre ranking table

This table contains historical data aggregated per genre, calculating weighted averaged ratings. The table stores all 
genre ratings per day accumulating all movies rated until processing date. A transformation example is shown below:

![alt text](./img/transformation_example.png "Transformation example")

Output table columns are shown below:

| Column name  | Column type | Description                                                |
|--------------|-------------|------------------------------------------------------------|
| date         | integer     | Date, in format YYYYMMDD. Column used as partition column. |
| genre_id     | string      | Genre ID.                                                  |
| genre_name   | double      | Genre name.                                                |
| vote_count   | double      | Number of total votes per genre until date.                |
| vote_average | integer     | Weighted average vote rating per genre until date.         |

# Windows Hadopp issues
If project it is being executed in Windows, be aware that it is necessary to install native libraries before proceed.
More info can be found in [official documentation](https://cwiki.apache.org/confluence/display/HADOOP2/WindowsProblems).
Winutils can be downloaded [here](https://github.com/cdarlint/winutils).
