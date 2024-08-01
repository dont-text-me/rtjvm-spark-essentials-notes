package part7bigdata

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TaxiEconomicImpact {

  def main(args: Array[String]): Unit = {
    //args: big data source, taxi zones data source, output destination
    if (args.length != 3) {
      println("Invalid number of arguments")
      System.exit(1)
    }

    val spark = SparkSession
      .builder()
      .appName("Taxi Big Data application")
      .config("spark.master", "local")
      .config("spark.log.level", "WARN") // too many logs at info level
      .getOrCreate()
    import spark.implicits._

    val bigTaxiDF =
      spark.read.load(args(0))

    val taxiZonesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(1))

    val percentGroupAttempt = 0.05
    val percentAcceptGrouping = 0.3
    val discountDollars = 5
    val extraCostDollars = 2
    val avgCostReduction =
      0.6 * bigTaxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)
    val percentGroupable =
      0.8726396760401696 // found by inspecting the little taxi dataframe

    val groupAttemptsDF = bigTaxiDF
      .select(
        round(unix_timestamp(col("pickup_datetime")) / (5 * 60))
          .cast("integer")
          .as("fiveMinuteId"),
        col("pickup_taxizone_id"),
        col("total_amount")
      )
      .groupBy(col("fiveMinuteId"), col("pickup_taxizone_id"))
      .agg(
        count("*") * percentGroupable as "totalTrips",
        sum(col("total_amount")) as "totalAmount"
      )
      .orderBy(col("totalTrips").desc_nulls_last)
      .withColumn(
        "approx_datetime",
        from_unixtime(col("fiveMinuteId") * (5 * 60))
      )
      .drop("fiveMinuteId")
      .join(taxiZonesDF, col("pickup_taxizone_id") === col("LocationID"))
      .drop("LocationID", "service_zone")

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
    val totalProfitDF = groupingEstimateEconomicImpactDF.select(
      sum(col("totalImpact")).as("total")
    )
    totalProfitDF.show()
    totalProfitDF.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save(args(3))
  }
}
