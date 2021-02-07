package com.guzelcihad.spark
package chapter3

import SparkObject.spark

object RowSample extends App {

  val rows = spark.createDataFrame(Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA")))
  val authorsDF = rows.toDF("Author", "State")
  authorsDF.show()
}
