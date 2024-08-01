package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App {
  val spark = SparkSession
    .builder()
    .appName("Taxi Big Data application")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()
  import spark.implicits._

  val taxiDF =
    spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  val lessThan3PassengersRatio = taxiDF.where(col("passenger_count") < 3).select(count("*")).as[Double].take(1)(0) / taxiDF.count()
  println(lessThan3PassengersRatio)

  val bigTaxiDF =
    spark.read.load("/Users/ivanbar/Downloads/NYC_taxi_2009-2016.parquet")

  bigTaxiDF.printSchema()

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zone_lookup.csv")

  taxiZonesDF.printSchema()

  val pickupsByZoneDF = bigTaxiDF
    .groupBy("pickup_taxizone_id")
    .agg(
      count("*").as("totalTrips")
    )
    .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)

  val pickupsByBorough = pickupsByZoneDF
    .groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  val pickupsByHourDF = bigTaxiDF
    .groupBy(hour(col("pickup_datetime")))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  val tripDistanceDF = taxiDF.select(col("trip_distance"))

  val longDistanceThreshold = 30 //miles
  val tripDistanceStats = tripDistanceDF.select(
    count("*") as "count",
    lit(longDistanceThreshold) as "threshold",
    mean("trip_distance") as "mean",
    stddev("trip_distance") as "stddev",
    min("trip_distance") as "min",
    max("trip_distance") as "max"
  )

  val tripsWithLengthDF =
    taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy("isLong").count()
  val pickupsByHourByLengthDF = tripsWithLengthDF
    .groupBy(hour(col("tpep_pickup_datetime")), col("isLong"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  def pickupDropoffPopularity(predicate: Column) = tripsWithLengthDF
    .where(predicate)
    .groupBy("PULocationID", "DOLocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Pickup_Zone")
    .drop("LocationID", "Borough", "service_zone")
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .withColumnRenamed("Zone", "Dropoff_Zone")
    .drop(
      "LocationID",
      "Borough",
      "service_zone",
      "PULocationID",
      "DOLocationID"
    )
    .orderBy(col("totalTrips").desc_nulls_last)

//  pickupDropoffPopularity(col("isLong")).show()
//  pickupDropoffPopularity(not(col("isLong"))).show()

  val rateCodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID"))
    .agg(count("*") as "totalTrips")
    .orderBy(col("totalTrips").desc_nulls_last)

  rateCodeDistributionDF.show()

  val rateCodeEvolution = bigTaxiDF
    .groupBy(
      to_date(col("pickup_datetime")).as("pickup_day"),
      col("rate_code_id")
    )
    .agg(
      count("*") as "totalTrips"
    )
    .orderBy("pickup_day")

  val groupAttemptsDF = taxiDF
    .select(
      round(unix_timestamp(col("tpep_pickup_datetime")) / (5 * 60))
        .cast("integer")
        .as("fiveMinuteId"),
      col("PULocationID"),
      col("total_amount")
    )
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinuteId"), col("PULocationID"))
    .agg(
      count("*") as "totalTrips",
      sum(col("total_amount")) as "totalAmount"
    )
    .orderBy(col("totalTrips").desc_nulls_last)
    .withColumn(
      "approx_datetime",
      from_unixtime(col("fiveMinuteId") * (5 * 60))
    )
    .drop("fiveMinuteId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")

  groupAttemptsDF.show()

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discountDollars = 5
  val extraCostDollars = 2
  val avgCostReduction =
    0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn(
      "groupedRides",
      col("totalTrips") * percentGroupAttempt
    )
    .withColumn(
      "acceptedGroupedRidesEconomicImpact",
      col(
        "groupedRides"
      ) * percentAcceptGrouping * (avgCostReduction - discountDollars)
    )
    .withColumn(
      "rejectedGroupedRidesEconomicImpact",
      col("groupedRides") * (1 - percentAcceptGrouping) * extraCostDollars
    )
    .withColumn(
      "totalImpact",
      col("acceptedGroupedRidesEconomicImpact") + col(
        "rejectedGroupedRidesEconomicImpact"
      )
    )

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))

}
