package com.guzelcihad.spark
package chapter3

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DefineSchema extends App {

  // There are two ways to define a schema. Programatically or emply a DDL.

  // Define programatically
  val schema = StructType(Array(StructField("author", StringType, false),
    StructField("title", StringType, false),
    StructField("pages", IntegerType, false)))

  // Define using DDL
  val schemaDDL = "author STRING, title STRING, pages INT"



}
