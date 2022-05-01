package org.rrajesh1979.learn

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object SparkBasics extends App {

  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("Spark Playground")
    .config("spark.master", "local")
//    .master("local[*]")
    .getOrCreate()

  // Create a DataFrame
  val df = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // Show the DataFrame
  df.show()

  // Print the schema
  df.printSchema()

  // Select the "Name" column
  df.select("Name").show()

  // Select the "Name" and "Miles_per_Gallon" columns
  df.select(df.col("Name"), df.col("Miles_per_Gallon")).show()

  // Cars Schema Struct
  val carsSchema = StructType(Array(
    StructField("Name", StringType, nullable = true),
    StructField("Miles_per_Gallon", DoubleType, nullable = true),
    StructField("Cylinders", LongType, nullable = true),
    StructField("Displacement", DoubleType, nullable = true),
    StructField("Horsepower", DoubleType, nullable = true),
    StructField("Weight_in_lbs", LongType, nullable = true),
    StructField("Acceleration", DoubleType, nullable = true),
    StructField("Year", StringType, nullable = true),
    StructField("Origin", StringType, nullable = true)
  ))

  // obtain a schema
  val carsDFSchema = df.schema

  // read a DF with your schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // Display the Schema
  carsDFWithSchema.printSchema()

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18,8,307,130,3504,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15,8,350,165,3693,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18,8,318,150,3436,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16,8,304,150,3433,12.0,"1970-01-01","USA"),
    ("ford torino",17,8,302,140,3449,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15,8,429,198,4341,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14,8,454,220,4354,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14,8,440,215,4312,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14,8,455,225,4425,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15,8,390,190,3850,8.5,"1970-01-01","USA")
  )
  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  //import spark.implicits._
  import spark.implicits._
  val manualCarsDFWithSchema =
    cars.toDF("Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin")

  // Display the Schema
  manualCarsDFWithSchema.printSchema()

  // Display the DataFrame
  manualCarsDFWithSchema.show()


  /**
    * Exercise:
    * 1) Create a manual DF describing smartphones
    *   - make
    *   - model
    *   - screen dimension
    *   - camera megapixels
    *
    * 2) Read another file from the data/ folder, e.g. movies.json
    *   - print its schema
    *   - count the number of rows, call count()
    */

  val smartphoneSchema = StructType(Array(
    StructField("make", StringType, nullable = true),
    StructField("model", StringType, nullable = true),
    StructField("screen_dimension", StringType, nullable = true),
    StructField("camera_megapixels", DoubleType, nullable = true)
  ))

  val smartPhones = Seq(
    ("Apple", "iPhone X", "5.8", 12.0),
    ("Apple", "iPhone 8", "4.7", 8.0),
    ("Apple", "iPhone 7", "4.7", 7.0),
    ("Apple", "iPhone 6", "4.0", 5.5),
    ("Apple", "iPhone 5", "3.5", 4.0),
    ("Apple", "iPhone 4", "2.5", 2.0),
    ("Apple", "iPhone 3", "2.0", 1.5),
    ("Apple", "iPhone 2", "1.5", 1.0),
    ("Apple", "iPhone 1", "1.0", 0.5),
    ("Apple", "iPhone", "0.5", 0.0),
    ("Samsung", "Galaxy S10", "6.5", 12.0),
    ("Samsung", "Galaxy S9", "6.0", 8.0),
    ("Samsung", "Galaxy S8", "5.5", 7.0),
    ("Samsung", "Galaxy S7", "5.0", 5.5),
    ("Samsung", "Galaxy S6", "4.7", 4.0),
    ("Samsung", "Galaxy S5", "4.0", 3.5),
    ("Samsung", "Galaxy S4", "3.5", 3.0),
    ("Samsung", "Galaxy S3", "3.0", 2.5),
    ("Samsung", "Galaxy S2", "2.5", 2.0),
    ("Samsung", "Galaxy S1", "2.0", 1.5),
    ("Samsung", "Galaxy S", "1.5", 1.0),
    ("Samsung", "Galaxy", "1.0", 0.5),
    ("Samsung", "Galaxy", "0.5", 0.0)
  )

  val smartphonesDF = smartPhones.toDF("make", "model", "screen_dimension", "camera_megapixels")
  smartphonesDF.printSchema()
  smartphonesDF.show()

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(s"Number of rows: ${moviesDF.count()}")


}
