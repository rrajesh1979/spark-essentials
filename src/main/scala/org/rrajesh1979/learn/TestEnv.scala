package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession

object TestEnv extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .master("local")
    .appName("SparkDataSources").getOrCreate()

  // Read movies json file
  val movies = spark.read.json("src/main/resources/data/movies.json")

  // Show the data
  movies.printSchema()
  movies.show()
}
