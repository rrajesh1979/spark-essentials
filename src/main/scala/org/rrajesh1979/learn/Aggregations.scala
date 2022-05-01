package org.rrajesh1979.learn

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{sum, avg, col, count, date_format}

object Aggregations extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .master("local")
    .appName("SparkDataSources").getOrCreate()

  // Read movies json file
  val movies = spark.read.json("src/main/resources/data/movies.json")

  // Show the data
  movies.printSchema()
  movies.show()

  // Group by Genre and count the number of movies
  movies
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").alias("Count"),
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      sum("US_Gross").as("Sum_US_Gross")
    )
    .orderBy(col("Avg_IMDB_Rating").desc)
    .show()

  // Filter by Genre
  val genreFilter = col("Major_Genre") === "Drama"

  // Filter by Rating
  val ratingFilter = col("IMDB_Rating") >= 7.0

  // Filter by Genre and Rating
  val genreRatingFilter = genreFilter and ratingFilter

  movies.filter(genreRatingFilter).show()
  movies.select(col("Title"), genreRatingFilter.as("flag"))
    .where(col("flag") === true).show()


}
