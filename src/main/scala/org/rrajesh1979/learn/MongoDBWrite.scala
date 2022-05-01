package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession
import com.mongodb.spark._

object MongoDBWrite extends App {
  val mongodbUri = "mongodb://localhost:27017/spark.movies"

  // SparkSession
  val spark = SparkSession.builder()
    .appName("MongoDBApp")
    .master("local[*]")
    .config("spark.mongodb.read.connection.uri", mongodbUri)
    .config("spark.mongodb.write.connection.uri", mongodbUri)
    .getOrCreate()

  // Read movies json file
  val movies = spark.read.json("src/main/resources/data/movies.json")

  // Write to MongoDB
  movies
    .write
    .format("mongodb")
    .mode("overwrite").save()


}
