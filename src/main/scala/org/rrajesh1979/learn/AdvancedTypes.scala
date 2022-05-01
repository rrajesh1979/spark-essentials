package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, col, current_date, current_timestamp, datediff, to_date}

object AdvancedTypes extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .master("local")
    .appName("SparkDataSources").getOrCreate()

  // Read movies json file
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // Show the data
  moviesDF.printSchema()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesWithDate = moviesDF
    .select(
      col("Title"),
      col("Release_Date"),
      to_date(col("Release_Date"), "dd-MMM-yy").as("ReleaseDate")
    )
    .withColumn("Today", current_date())
    .withColumn("CurrentTime", current_timestamp())
    .withColumn("MovieAge", datediff(col("Today"), col("ReleaseDate")) / 365)

  //Null handling
  val moviesWithOutNull = moviesDF
    .select(
      "Title",
      "IMDB_Rating"
    )
    .na
    .fill(Map("IMDB_Rating" -> 0))

  moviesWithOutNull.show()


  //Coalesce example
  moviesDF
    .select(
      col("Title"),
      col("Rotten_Tomatoes_Rating"),
      col("IMDB_Rating"),
      coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10).as("Rating")
    )
    .show

}
