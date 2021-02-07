package com.guzelcihad.spark
package chapter3

import com.guzelcihad.spark.chapter3.FirstExample.spark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object RowSample extends App {

  val spark = SparkSession
    .builder()
    .appName("AuthorAges")
    .getOrCreate()

  val rows = spark.createDataFrame(Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA")))
  val authorsDF = rows.toDF("Author", "State")
  authorsDF.show()
}
