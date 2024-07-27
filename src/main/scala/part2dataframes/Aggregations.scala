package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession
    .builder()
    .appName("Aggregations and grouping")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", value = true)
    .load("src/main/resources/data/movies.json")

  val genreCountsDF = movieDF.select(countDistinct(col("Major_Genre")))
  //movieDF.selectExpr("count(Major_Genre)")

  //genreCountsDF.show()

  val perGenreCountsDF =
    movieDF.groupBy("Major_Genre").count().where(col("Major_Genre").isNotNull)
  //perGenreCountsDF.show()

  val aggregationsByGenreDF = movieDF
    .groupBy("Major_Genre")
    .agg(
      count("*") as "Num_Movies",
      avg("IMDB_Rating") as "Avg_rating"
    )
    .orderBy("Avg_rating")
  //aggregationsByGenreDF.show()

  val totalProfits = movieDF.select(
    sum(
      col("US_Gross") + col("Worldwide_Gross") + when(
        col("US_DVD_Sales").isNull,
        0
      ).otherwise(col("US_DVD_Sales"))
    ).as("Overall_earnings")
  )
  totalProfits.show()

  val distinctDirectors = movieDF.select(countDistinct("Director"))
  distinctDirectors.show()

  movieDF
    .select(
      mean("US_Gross") as "Mean earnings in the US",
      stddev("US_Gross") as "Standard distribution of earnings in the US"
    )
    .show()

  movieDF
    .groupBy("Director")
    .agg(
      mean("US_Gross") as "Mean US revenue",
      mean("IMDB_Rating") as "Mean IMDB rating"
    )
    .orderBy(col("Mean IMDB rating").desc)
    .show()
}
