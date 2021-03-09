package com.guzelcihad.spark
package chapter7

import SparkObject.spark

import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Persistence extends App {

  val df = spark.range(1 * 10000000)
    .toDF("id")
    .withColumn("square", expr("id * id"))
    .repartition(12)

  df.persist(StorageLevel.DISK_ONLY)  // serialize the data and cache it on disk
  df.count()  // materialize the cache

  df.count()  // now get it from cache
}
