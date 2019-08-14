package org.after90.spark

import org.apache.spark.sql.SparkSession

/**
  * Created by zhaogj on 30/03/2017.
  */
object InitializingSparkSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()

    spark.stop()
  }
}
