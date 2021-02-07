package com.guzelcihad.spark
package chapter4

import SparkObject.spark

object SparkSqlCreateDb extends App {

  spark.sql("CREATE DATABASE learn_spark_db")
  spark.sql("USE learn_spark_db")

  spark.sql("CREATE TABLE managed_us_delay_flights_tbl (date STRING," +
    "delay INT, distance INT, origin STRING, destination STRING)")

  /** We can do this also with DataFrame API
  val csv_File = "path"
  schema = "date STRING, etc"
  flights_df = spark.read.csv(csv_File, schema=schema)
  flights_df.write.saveAsTable("managed_us_delay_flights_tbl")
  **/


}
