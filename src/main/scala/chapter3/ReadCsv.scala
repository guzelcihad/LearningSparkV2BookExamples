package com.guzelcihad.spark
package chapter3

import SparkObject.spark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ReadCsv extends App {

  val fireSchema = StructType(Array(StructField("CallNumber", IntegerType, true),
    StructField("UnitId", StringType, true),
    StructField("IncidentNumber", IntegerType, true),
    StructField("CallType", StringType, true),
    StructField("CallDate", StringType, true),
    StructField("WatchDate", StringType, true),
    StructField("CallFinalDisposition", StringType, true),
    StructField("AvailableDtTm", StringType, true),
    StructField("Address", StringType, true),
    StructField("City", StringType, true),
    StructField("Zipcode", IntegerType, true),
    StructField("Battalion", StringType, true),
    StructField("StationArea", StringType, true),
    StructField("Box", StringType, true),
    StructField("OriginalPriority", StringType, true),
    StructField("Priority", StringType, true),
    StructField("FinalPriority", IntegerType, true),
    StructField("ALSUnit", BooleanType, true),
    StructField("CallTypeGroup", StringType, true),
    StructField("NumAlarms", IntegerType, true),
    StructField("UnitType", StringType, true),
    StructField("UnitSequenceInCallDispatch", IntegerType, true),
    StructField("FirePreventionDistrict", StringType, true),
    StructField("SupervisorDistrict", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Location", StringType, true),
    StructField("RowID", StringType, true),
    StructField("Delay", FloatType, true)))

  val sfFireFile = "/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/sf-fire-calls.csv"

  val fireDF = spark
    .read
    .schema(fireSchema)
    .option("header", "true")
    .csv(sfFireFile)

  fireDF.printSchema()

  val fewFireDF = fireDF
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType").=!=("Medical Incident"))

  fewFireDF.show(5, false)

  // How many distinct col types recorded
  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .agg(countDistinct(col("CallType")).as("DistinctCallTypes"))
    .show()

  // List distinct call types
  fireDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .distinct()
    .show(10, false)

  // Rename a column
  val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
  newFireDF
    .select("ResponseDelayedinMins")
    .where("ResponseDelayedinMins > 1")
    .show(5, false)

  // Change type of data
  val fireTsDF = newFireDF
    .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
    .drop("CallDate")
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
    .drop("WatchDate")
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
    .drop("AvailableDtTm")

  fireTsDF
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, false)

  // Which years is the dataset includes
  fireTsDF
    .select(year(col("IncidentDate")))
    .distinct()
    .orderBy(year(col("IncidentDate")))
    .show()

  // What were the most common types of fire calls
  fireTsDF
    .select("CallType")
    .where(col("CallType").isNotNull)
    .groupBy("CallType")
    .count()
    .orderBy(desc("count"))
    .show(10, false)

  /* We can also use collect() method but do not use
  * it returns collection of all records that can be cause out of memory exceptions.
  * We can also use take(n) to get collection of n records */

  fireTsDF
    .select(sum("NumAlarms"),
      avg("ResponseDelayedinMins"),
      min("ResponseDelayedinMins"),
      max("ResponseDelayedinMins"))
    .show()
}
