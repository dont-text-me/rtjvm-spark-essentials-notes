package part3typesDatasets

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object Datasets extends App {
  val spark = SparkSession
    .builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val numbersDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()
  implicit val intEncoder: Encoder[Int] = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  // dataset of a complex type (car)

  case class Car(
      Name: String,
      Miles_Per_Gallon: Option[Double],
      Cylinders: Long,
      Displacement: Double,
      Horsepower: Option[Long],
      Weight_in_lbs: Long,
      Acceleration: Double,
      Year: String,
      Origin: String
  )

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")
  import spark.implicits._
  val carsDS = carsDF.as[Car]

  val carNamesDS = carsDS.map(car => car.Name.toUpperCase)
  carNamesDS.show

  println(carsDS.count())
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsDS.count())

  case class Guitar(id: Long, make: String, guitarType: String)
  case class GuitarPlayer(
      id: Long,
      name: String,
      guitars: Seq[Long],
      band: Long
  )
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS = guitarPlayersDS.joinWith(
    bandsDS,
    guitarPlayersDS.col("band") === bandsDS.col("id")
  )
  guitarPlayerBandsDS.show()

  val guitarsAndPlayersDS = guitarsDS.joinWith(
    guitarPlayersDS,
    array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")),
    "outer"
  )
  guitarsAndPlayersDS.show()

  val carsByOrigin = carsDS.groupByKey(_.Origin).count().show()
}
