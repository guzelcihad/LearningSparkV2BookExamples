package com.guzelcihad.spark
package chapter5

import SparkObject.spark

import org.apache.spark.sql.functions.expr

object TransformationExamples extends App {

  import spark.sqlContext.implicits._

  val delaysPath = "/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/departuredelays.csv"

  val airportsPath = "/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/airport-codes-na.txt"

  val airports = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", "\t")
    .csv(airportsPath)

  airports.createOrReplaceTempView("airports_na")

  val delays = spark
    .read
    .option("header", "true")
    .csv(delaysPath)
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance"))

  delays.createOrReplaceTempView("departureDelays")

  // Create temp small table
  val foo = delays.filter(expr(
    """origin == 'SEA' AND destination == 'SFO'
      AND date like '01010%' AND delay > 0
      """))
  foo.createOrReplaceTempView("foo")

  spark.sql("SELECT * FROM airports_na LIMIT 10").show()

  spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

  spark.sql("SELECT * FROM foo").show()

  // union two tables
  val bar = delays.union(foo)
  bar.createOrReplaceTempView("bar")
  bar.filter(expr(
    """origin == 'SEA' AND destination == 'SFO'
      AND date like '01010%' AND delay > 0
      """)).distinct().show()

  foo.join(
    airports.as('air),
    $"air.IATA" === $"origin"
  ).select("City", "State", "date", "delay", "distance", "destination")
    .show()
}

