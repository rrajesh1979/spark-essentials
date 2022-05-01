name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.2.1"
val vegasVersion = "0.3.11"
val postgresVersion = "42.3.4"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.17.2",
  "org.apache.logging.log4j" % "log4j-core" % "2.17.2",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % postgresVersion,
  // MongoDB for DB connectivity
  "org.mongodb.spark" % "mongo-spark-connector" % "10.0.0"
)