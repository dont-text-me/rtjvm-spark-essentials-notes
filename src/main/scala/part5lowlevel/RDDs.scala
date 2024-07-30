package part5lowlevel

import org.apache.hadoop.shaded.org.eclipse.jetty.util.ajax.JSON
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.immutable.HashMap
import scala.io.Source

object RDDs extends App {

  val spark = SparkSession
    .builder()
    .appName("RDD intro")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val sc = spark.sparkContext
  val numbers = 1 to 1000000
  val numbersRDD = sc.parallelize(numbers)

  case class StockValue(symbol: String, date: String, price: Double)
  def readStocks(filename: String) = Source
    .fromFile(filename)
    .getLines()
    .drop(1) // header
    .map(_.split(","))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toList

  val stocksRDD =
    sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

  val msftRDD = stocksRDD.filter(_.symbol == "MSFT") //lazy
  val mscount = msftRDD.count() // eager

  val companyNamesRDD = stocksRDD.map(_.symbol).distinct()
  implicit val stockOrdering =
    Ordering.fromLessThan[StockValue]((sa, sb) => sa.price < sb.price)
  val minMSFT = msftRDD.min()

  numbersRDD.reduce(_ + _)

  val groupedStocksRDD = stocksRDD.groupBy(_.symbol)

  val repartitionedStockRDD = stocksRDD.repartition(30)
  spark
    .createDataFrame(repartitionedStockRDD)
    .write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/stocks30")

  val coalescedRDD =
    repartitionedStockRDD.coalesce(15) // does not involve shuffling

  case class Movie(title: String, genre: String, rating: Double)

  def parseJson(filename: String) = Source
    .fromFile(filename)
    .getLines()
    .map(line => JSON.parse(line).asInstanceOf[java.util.HashMap[String, Any]])
    .map(json =>
      Movie(
        Option(json.get("Title")).getOrElse("Unknown").toString,
        Option(json.get("Major_Genre")).getOrElse("Unknown").toString,
        Option(json.get("IMDB_Rating")).getOrElse(0.0).toString.toDouble
      )
    )
    .toList

  val movieRDD =
    sc.parallelize(parseJson("src/main/resources/data/movies.json"))

  val genresRDD = movieRDD.map(_.genre).distinct()
  val goodMovies = movieRDD.filter(_.rating > 6.0)
  val avgByGenre = movieRDD
    .groupBy(_.genre)
    .map({ case (genre, movies) =>
      (genre, movies.map(_.rating).sum / movies.size)
    })

  spark.createDataFrame(goodMovies).show
  spark.createDataFrame(avgByGenre).show
}
