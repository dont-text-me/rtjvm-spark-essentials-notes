package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  val spark = SparkSession
    .builder()
    .appName("Managing nulls")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movieDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce( // select the first non-null value
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating") * 10
    )
  )

  movieDF.select("Title", "IMDB_Rating").na.drop()
  movieDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))
  movieDF.na.fill(
    Map(
      "IMDB_Rating" -> 0,
      "Rotten_Tomatoes_Rating" -> 10,
      "Director" -> "Unknown"
    )
  )

  movieDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10)" // same as coalesce, nvl does the same thing
  )
}
