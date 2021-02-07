package com.guzelcihad.spark
package chapter4

import SparkObject.spark

object SparkSqlExample extends App {

  val csvFile = "/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/departuredelays.csv"

  val df = spark
    .read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", true)
    .load(csvFile)

  df.createOrReplaceTempView("us_delay_flights_tbl")

  spark.sql(
    """SELECT distance, origin, destination
      FROM us_delay_flights_tbl WHERE distance > 1000
      ORDER BY distance DESC
      """).show(10)

}
