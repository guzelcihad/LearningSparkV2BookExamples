package com.guzelcihad.spark
package chapter5

import SparkObject.spark

object SparkSqlUDF extends App {

  // Create a function
  val cubed = (s: Long) => s * s * s

  // Register as UDF
  spark.udf.register("cubed", cubed)

  // Create a temp view
  spark.range(1, 9).createOrReplaceTempView("udf_test")

  spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
}
