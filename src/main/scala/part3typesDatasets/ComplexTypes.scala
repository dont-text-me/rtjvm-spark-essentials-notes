package part3typesDatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("Complex Data types")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  val moviesWithReleaseDates = movieDF
    .select(
      col("Title"),
      to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")
    )
    .withColumn("Today", current_date())
    .withColumn(
      "Movie_Age",
      datediff(col("Today"), col("Actual_Release")) / 365
    )

  val stockDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  stockDF
    .select(col("symbol"), to_date(col("date"), "MMM d yyyy").as("Stock_Date"))
    .show()

  movieDF
    .select(
      col("Title"),
      struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
    )
    .select(col("Title"), col("Profit").getField("US_Gross"))
    .show()

  val moviesWithWords =
    movieDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words"))

  moviesWithWords
    .select(
      col("Title"),
      expr("Title_Words[0]"),
      size(col("Title_Words")),
      array_contains(col("Title_Words"), "Love")
    )
    .show()
}
