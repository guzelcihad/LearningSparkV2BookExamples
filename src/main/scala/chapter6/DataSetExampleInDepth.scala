package com.guzelcihad.spark
package chapter6

import SparkObject.spark

import org.apache.spark.sql.functions.desc
import spark.sqlContext.implicits._

import scala.util.Random

object DataSetExampleInDepth extends App {

  val r = new Random(42)

  val data = for (i <- 0 to 1000)
    yield  Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
      r.nextInt(1000))

  val dsUsage = spark.createDataset(data)
  dsUsage.show()

  dsUsage
    .filter(_.usage > 900)
    .orderBy(desc("usage"))
    .show(5, false)

  dsUsage
    .map(u => {
      if (u.usage > 750) u.usage * .15
      else u.usage * .50
    }).show(5, false)

  def computeUserCostUsage(u: Usage): UsageCost = {
    val v = if (u.usage > 750) u.usage * 0.015 else u.usage * 0.50
      UsageCost(u.uid, u.uname, u.usage, v)
  }

  dsUsage.map(computeUserCostUsage(_)).show(5, false)
}

// uid: unique id, uname: randomly generated user name, usage: minutes of server usage
case class Usage(uid: Int, uname: String, usage: Int)

case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)


