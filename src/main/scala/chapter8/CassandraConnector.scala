package com.guzelcihad.spark
package chapter8

import SparkObject.spark

import org.apache.spark.sql.DataFrame

object CassandraConnector extends App {

  val hostAddr = ""
  val keySpaceName = ""
  val tableName = ""

  spark.conf.set("spark.cassandra.connection.host", hostAddr)

  def writeDfToCassandra(df: DataFrame, batchId: Long) =
    df.write
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> tableName, "keyspace" -> keySpaceName))
      .mode("append")
      .save()

  def writeDfToMultipleLocations(df: DataFrame, batchId: Long) = {
    df.persist()
    // df.write.format().save()  // Location 1
    // df.write.format().save()  // Location 2
    df.unpersist()
  }

  val streamingQuery = spark.emptyDataFrame
    .writeStream
    .foreachBatch(writeDfToCassandra _)
    .outputMode("update")
    .option("checkpointLocation", String)
    .start()
}
