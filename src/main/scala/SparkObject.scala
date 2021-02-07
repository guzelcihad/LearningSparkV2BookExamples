package com.guzelcihad.spark

import org.apache.spark.sql.SparkSession

object SparkObject {

  val spark = SparkSession
    .builder()
    .getOrCreate()
}
