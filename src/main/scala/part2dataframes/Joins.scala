package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App {
  val spark = SparkSession
    .builder()
    .appName("Joins")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")
  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")
  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  //guitaristsBandsDF.show()
  // semi-join = everything in the left DF for which there is a row in the right DF satisfying the condition
  // anti-join = everything in hte left DF for which there is NO row in the right DF satisfying the condition
  //guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .option("dateFormat", "YYYY-MM-dd") // if parsing fails, date will be null
    .load()
  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .option("dateFormat", "YYYY-MM-dd") // if parsing fails, date will be null
    .load()

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.titles")
    .option("dateFormat", "YYYY-MM-dd") // if parsing fails, date will be null
    .load()
  // show the max salary of each employee
  val maxSalaryDF = employeesDF
    .join(
      salariesDF,
      "emp_no",
      "left"
    )
    .drop(salariesDF.col("emp_no"))
    .groupBy("emp_no")
    .agg(max(col("salary")) as "Max_Salary")
  maxSalaryDF.show()

  // show all employees who were never managers
  employeesDF
    .join(
      titlesDF.where(col("title") === "Manager"),
      "emp_no",
      "left_anti"
    )
    .show()

  // job titles of the best paid 10 employees in the company

  maxSalaryDF
    .orderBy(col("Max_Salary").desc)
    .limit(10)
    .join(
      titlesDF
        .groupBy("emp_no")
        .agg(
          max_by(col("title"), col("from_date"))
        ), // only include the latest title of each employee
      "emp_no",
      "left"
    )
    .drop(titlesDF.col("emp_no"))
    .show()
}
