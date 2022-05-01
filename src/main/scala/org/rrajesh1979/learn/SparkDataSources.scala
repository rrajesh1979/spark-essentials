package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.types._

object SparkDataSources extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("SparkDataSources").getOrCreate()

  // schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // read data from json file
  val carsDF = spark.read
    .schema(carsSchema)
    .option("mode", "FAILFAST")
    .option("dateFormat", "yyyy-MM-dd")
    .json("src/main/resources/data/cars.json")

  carsDF.printSchema()
  carsDF.show()

  // write data to parquet file
  carsDF.write
    .mode("overwrite")
    .parquet("src/main/resources/data/cars.parquet")

  // read data from parquet file
  val carsParquetDF = spark.read
    .schema(carsSchema)
    .parquet("src/main/resources/data/cars.parquet")

  carsParquetDF.printSchema()
  carsParquetDF.show()

  // read data from stocks csv file
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

//  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val stocksDF = spark.read
    .schema(stocksSchema)
    .option("header", "true")
//    .option("mode", "FAILFAST")
    .option("dateFormat", "MMM d yyyy")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.printSchema()
  stocksDF.show()

  // carsDF with kgs
  val carsDFWithKgs =
    carsDF.withColumn("Weight_in_kgs", carsDF("Weight_in_lbs") / 2.20462262)

  carsDFWithKgs.printSchema()
  carsDFWithKgs.show()

  // carsDF with Name, Year and Origin
  val carsDFWithNameYearOrigin =
    carsDF.select(
      col("Name"),
      col("Year"),
      col("Origin"),
      expr("Weight_in_lbs / 2.2").alias("Weight_in_kgs")
    )

  carsDFWithNameYearOrigin.printSchema()
  carsDFWithNameYearOrigin.show()

  // Filter carsDF with weight > 2000
  val carsDFWithWeightGreaterThan2000 =
    carsDF.filter(col("Weight_in_lbs") > 2000)

  carsDFWithWeightGreaterThan2000.show()

  // Filter Euro cars
  val euroCarsDF = carsDF.filter(col("Origin") === "Europe")

  euroCarsDF.show()

  // Filter chaining - US cars with horsepower > 150
  val usCarsDFChain = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  val usCarsDF = carsDF.filter(
    col("Origin") === "USA" and col("Horsepower") > 150)
    .sort(col("Horsepower").desc)

  usCarsDF.show()

  // Group by Origin
  val carsGroupByOriginDF = carsDF.groupBy(col("Origin")).count()

  carsGroupByOriginDF.show()

  // Read movies json file
  val moviesSchema = StructType(
    Array(
      StructField("Title", StringType),
      StructField("US_Gross", DoubleType),
      StructField("Worldwide_Gross", DoubleType),
      StructField("US_DVD_Sales", DoubleType),
      StructField("Production_Budget", DoubleType),
      StructField("Release_Date", DateType),
      StructField("MPAA_Rating", StringType),
      StructField("Running_Time_min", LongType),
      StructField("Distributor", StringType),
      StructField("Source", StringType),
      StructField("Major_Genre", StringType),
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Rotten_Tomatoes_Rating", DoubleType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", LongType),
    )
  )

  val moviesDF = spark.read
    .schema(moviesSchema)
    .option("mode", "FAILFAST")
    .option("dateFormat", "d-MMM-yy")
    .json("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  moviesDF.show()

  // Take Title, Release Date, US_Gross, Worldwide_Gross, Director
  // Filter Comedy movies
  // Filter movies with US_Gross > 1_000_000_000
  // Sort by US_Gross
  val moviesTitleGrossDirectorDF = moviesDF
    .filter(col("Major_Genre") === "Comedy")
    .filter(col("US_Gross") > 100000000)
    .select(
      col("Title"),
      col("Release_Date"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      col("Director")
    )
    .withColumn(
      "Total_Gross",
      col("US_Gross") + col("Worldwide_Gross")
    )
    .sort(col("Total_Gross").desc)
  
  moviesTitleGrossDirectorDF.show()



}
