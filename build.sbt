name := "spark-essentials"

version := "0.1"

scalaVersion := "2.13.14"

val sparkVersion = "4.0.0-preview1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.7.3"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.23.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.23.1",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion
)
