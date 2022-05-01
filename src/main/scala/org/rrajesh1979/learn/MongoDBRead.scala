package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession

import java.sql.Date

object MongoDBRead extends App {
  val mongodbUri = "mongodb://localhost:27017/spark.movies"

  // SparkSession
  val spark = SparkSession.builder()
    .appName("MongoDBApp")
    .master("local")
    .config("spark.mongodb.read.connection.uri", mongodbUri)
    .config("spark.mongodb.write.connection.uri", mongodbUri)
    .getOrCreate()

  // Read from MongoDB
  val moviesDF = spark.read
    .format("mongodb")
    .option("database", "spark")
    .option("collection", "movies")
    .load()

  moviesDF.printSchema()
  moviesDF.show()

  // Movies Case Class
  case class Movie(
    Title: String,
    US_Gross: Long,
    Worldwide_Gross: Long,
    US_DVD_Sales: Option[Long],
    Production_Budget: Long,
    Release_Date: String,
    MPAA_Rating: String,
    Running_Time_min: Option[Long],
    Distributor: String,
    Source: Option[String],
    Major_Genre: Option[String],
    Creative_Type: Option[String],
    Director: Option[String],
    Rotten_Tomatoes_Rating: Option[Double],
    IMDB_Rating: Option[Double],
    IMDB_Votes: Option[Long]
  )
  import spark.implicits._
  val moviesDS = moviesDF.as[Movie]

  moviesDS.printSchema()
  moviesDS.show()

  spark.close()

  System.exit(0)

}
