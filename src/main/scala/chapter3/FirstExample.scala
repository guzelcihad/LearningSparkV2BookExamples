package com.guzelcihad.spark
package chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object FirstExample extends App {

  // Create a DataFrame using SparkSession
  val spark = SparkSession
    .builder()
    .appName("AuthorAges")
    .getOrCreate()

  //Create a DataFrame of names and ages
  val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25),
    ("Denny", 31), ("Jules", 30), ("TD", 35))).toDF("name", "age")

  // Group the same names togetger, aggregate their ages, and compute an average
  val avgDF = dataDF.groupBy("name").agg(avg("age"))

  //Show the results of the final execution
  avgDF.show()
}
