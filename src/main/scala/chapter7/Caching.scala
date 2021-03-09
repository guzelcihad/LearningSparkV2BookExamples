package com.guzelcihad.spark
package chapter7

import SparkObject.spark
import org.apache.spark.sql.functions._

object Caching extends App {

  val df = spark.range(1 * 10000000)
    .toDF("id")
    .withColumn("square", expr("id * id"))
    .repartition(12)

  df.cache()  // cache the data
  df.count()  // materialize the cache

  df.count()  // now get it from cache

  System.in.read()
  spark.stop()
}
