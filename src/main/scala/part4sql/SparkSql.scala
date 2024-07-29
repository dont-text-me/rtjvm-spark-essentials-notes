package part4sql

import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSql extends App {
  val spark = SparkSession
    .builder()
    .appName("Spark sql practice")
    .config("spark.master", "local")
    .config("spark.log.level", "WARN") // too many logs at info level
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")

  val carsDF = readDF("cars.json")

  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )
  americanCarsDF.show()

  spark.sql("create database rtjvm")
  spark.sql("use rtjvm")

  val databasesDF = spark.sql("show databases")
  databasesDF.show()

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(
      tableNames: List[String],
      shouldWriteToWarehouse: Boolean = false
  ) = tableNames.foreach { name =>
    val tableDF = readTable(name)
    tableDF.createOrReplaceTempView(name)
    if (shouldWriteToWarehouse) {
      tableDF.write.mode(SaveMode.Overwrite).saveAsTable(name)
    }
  }

  transferTables(
    List(
      "employees",
      "departments",
      "titles",
      "dept_emp",
      "salaries",
      "dept_manager"
    )
  )
  spark
    .sql("""
      |select count(*) from employees where year(hire_date) = 2000
      |""".stripMargin)
    .show()
  spark
    .sql(
      """
      |select d.dept_no, avg(s.salary)
      |from employees e left join dept_emp d using (emp_no)
      |left join salaries s using (emp_no)
      |group by dept_no
      |""".stripMargin
    )
    .show()

  spark
    .sql(
      """
        |select d.dept_no, avg(s.salary) as mean_salary
        |from employees e left join dept_emp d using (emp_no)
        |left join salaries s using (emp_no)
        |group by dept_no order by mean_salary desc limit 1
        |""".stripMargin
    )
    .show()

}
