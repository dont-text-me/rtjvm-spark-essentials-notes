package part3typesDatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object CommonTypes extends App {
  val spark = SparkSession
    .builder()
    .appName("Common spark types")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  movieDF.select(col("Title"), lit(47) as "plain_value").show()

  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  val moviesWithGoodnessFlagsDF = movieDF.select(
    col("Title"),
    preferredFilter.as("Good_movie")
  ) // boolean flag on movies that match the filter

  val moviesWithoutGoodnessFlags =
    moviesWithGoodnessFlagsDF where not(col("Good_movie"))
  moviesWithGoodnessFlagsDF.where("Good_movie").show()

  // correlation

  println(
    movieDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")
  ) // corr is an action, is NOT lazy

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  val regexString = "volkswagen|vw"
  val vwDF = carsDF
    .select(
      col("Name"),
      regexp_extract(col("Name"), regexString, 0).as("regex_extract")
    )
    .where(col("regex_extract") =!= "")
  vwDF
    .select(
      col("Name"),
      regexp_replace(col("Name"), regexString, "veedoubleu").as("regex_extract")
    )
    .show()

  def getCarNames: List[String] = List("volkswagen", "chevrolet")

  val carNamesRegex = getCarNames.map(_.toLowerCase).mkString("|")

  val regexCol =
    regexp_extract(col("Name"), carNamesRegex, 0).as("regex_extract")
  carsDF.select(col("Name"), regexCol).where(col("regex_extract") =!= "").show()

  val carNamesFilter = getCarNames.foldRight(lit(false)) { (currName, statementSoFar) =>
    statementSoFar or (col("Name") contains currName.toLowerCase)
  }
  carsDF.select(col("Name")).where(carNamesFilter).show()
}
