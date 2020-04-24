package org.after90.spark.structuredStreaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWindowed {
  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("StructuredNetworkWordCount").setMaster("local")
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .option("includeTimestamp", true)
      .load()

    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    val windowedCounts = words.groupBy(
      window($"timestamp", "10 seconds", "10 seconds"),
      $"word"
    ).count().orderBy("window")

    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
