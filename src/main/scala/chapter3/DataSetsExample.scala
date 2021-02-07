package com.guzelcihad.spark
package chapter3

import SparkObject.spark

object DataSetsExample extends App {

  import spark.sqlContext.implicits._

  val ds = spark
    .read
    .json("/home/ubuntu/IdeaProjects/LearningSpark/src/main/scala/resources/iot_devices.json")
    .as[DeviceIOTData]

  ds.show(5, false)

  val filterTempDS = ds
    .filter(d => d.temp > 30 && d.humidity > 70)

  val filterDS = ds
    .select($"temp", $"deviceName", $"deviceId", $"cca3")
    .where("temp > 25")

  filterTempDS.show(5, false)
  filterDS.show(5, false)

}

// Easiest way to define a schema is using case class. Its like java bean
case class DeviceIOTData(battery_level: Long, c02_level: Long, cca2: String,
                         cca3: String, cn: String, device_id: Long,
                         device_name: String, humidity: Long, ip: String,
                         latitude: Double, lcd: String, longitude: Double,
                         scale: String, temp: Long, timestamp: Long)
