package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession
    .builder()
    .appName("DF columns and expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  val firstColumn = carsDF.col("Name")

  val carNamesDF = carsDF.select(firstColumn)

  carsDF.select(
    col("Name"),
    col("Acceleration"),
    expr("Origin") // expression
  )

  val weightInKGExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKGExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  val carsWithColumnRenamedDF = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")

  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  val movieDF = spark.read
    .format("json")
    .option("inferSchema", value = true)
    .load("src/main/resources/data/movies.json")


  val comediesDF = movieDF.select("Title", "IMDB_Rating", "Release_Date").where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val overallProfitDF = movieDF.select(col("Title"), (col("Worldwide_Gross") + col("US_Gross")).as("Overall_Gross"))
  comediesDF.show()
  overallProfitDF.show()

}
