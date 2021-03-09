package com.guzelcihad.spark
package chapter2

import SparkObject.spark
import org.apache.spark.sql.functions._

object CountMnm extends App{

    val mnm_df= spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/mnm_dataset.csv")

    val count_mnm_df = mnm_df
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
      .show(60,false)

    val ca_count_mnm_df = mnm_df
      .select("State", "Color", "Count")
      .where("State == 'CA'")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))
      .show(60,false)

}
