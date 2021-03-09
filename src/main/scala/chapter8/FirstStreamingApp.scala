package com.guzelcihad.spark
package chapter8

import SparkObject.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

object FirstStreamingApp extends App {

  spark.sparkContext.setLogLevel("ERROR")

  val lines = spark
    .readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()

  val filteredLines = lines.filter("isCorruptedUdf(value) = false")
  val words = lines.select(split(col("value"), "\\s").alias("word"))
  val counts = words.groupBy("word").count()

  val checkpointDir = "home/ubuntu/IdeaProjects/LearningSpark/resources/tmp/checkpoint"
  val streamingQuery = counts
    .writeStream
    .format("console")
    .outputMode("complete")
    .trigger(Trigger.ProcessingTime("1 second"))
    .option("checkpointLocation", checkpointDir)
    .start()

  streamingQuery.awaitTermination()
}
