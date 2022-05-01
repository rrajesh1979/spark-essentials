package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DeployApp {
  def main(args: Array[String]): Unit = {
    println("Starting the application")

    /**
      * Movies.json as args(0)
      * GoodComedies.json as args(1)
      *
      * good comedy = genre == Comedy and IMDB > 6.5
      */

    if (args.length != 2) {
      println("Need input path and output path")
      System.exit(1)
    }

    //Sparksession
    val spark = SparkSession
      .builder()
      .appName("DeployApp")
      .getOrCreate()

    //Read movies.json
    val movies = spark.read.json(args(0))

    //Create GoodComedies DF
    val goodComedies =
      movies
        .filter(movies("Major_Genre") === "Comedy" && movies("IMDB_Rating") > 6.5)
        .select(
          col("Title"),
          col("IMDB_Rating").as("Rating"),
          col("Major_Genre").as("Genre"),
          col("Release_Date").as("Released")
        )
        .orderBy(col("Rating").desc)

    //Display good comedies
    goodComedies.show()

    //Write to GoodComedies.json
    goodComedies.write.json(args(1))

  }
}
