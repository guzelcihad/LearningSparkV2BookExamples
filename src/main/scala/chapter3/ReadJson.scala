package com.guzelcihad.spark
package chapter3

import SparkObject.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadJson extends App {

  if (args.length <= 0) {
    println("Usage example <file path to blogs.json>")
    System.exit(1)
  }

  // Get the path to the JSON file
  val jsonFile = args(0)

  // Define our schema programmatically
  val schema = StructType(Array(StructField("Id", IntegerType, false),
    StructField("First", StringType, false),
    StructField("Last", StringType, false),
    StructField("Url", StringType, false),
    StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),
    StructField("Campaigns", ArrayType(StringType), false)))

  // Create a DataFrame by reading from the JSON file
  // with a predefined schema
  val blogsDF = spark
    .read
    .schema(schema)
    .json(jsonFile)

  // Show the DataFrame schema as output
  blogsDF.show(false)

  // Print the schema
  println(blogsDF.printSchema())
  println(blogsDF.schema)

  blogsDF.columns

  // Access a particular column
  blogsDF.col("Id")

  // Use expression to compute value
  blogsDF.select(expr("Hits * 2")).show(2)

  // or use col to compute value
  blogsDF.select(col("Hits") * 2).show(2)

  // Adds column based on conditional expression
  blogsDF.withColumn("Big Hitters", expr("Hits > 10000")).show()

  // Concatenate three columns, create a new column, and show the
  // newly created concatenated column
  blogsDF
    .withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))
    .select(col("AuthorsId"))
    .show(4)

  // These statements return the same value, showing that
  // expr is the same as a col method call
  blogsDF.select(expr("Hits")).show(2)
  blogsDF.select(col("Hits")).show(2)
  blogsDF.select("Hits").show(2)

  // Sort by column "Id" in descending order
  blogsDF.sort(col("Id").desc).show()
  //blogsDF.sort($"Id".desc).show()
}
