package org.rrajesh1979.learn

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object ComplexFilters extends App {
  // Create a SparkSession
  val spark = SparkSession.builder()
    .master("local")
    .appName("SparkDataSources").getOrCreate()

  // Read movies json file
  val carsDF = spark.read.json("src/main/resources/data/cars.json")

  // Show the data
  carsDF.printSchema()
  carsDF.show()

  // Filter the data
  def getCarNames: List[String] = List("BMW", "Audi", "Mercedes")
  val carNameFilters = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))

  println("Lower case car names: " + getCarNames.map(_.toLowerCase))
  println("Car Name Filters: " + carNameFilters)

  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDF.filter(bigFilter).show



}
