package org.after90.spark.sql.streaming

import java.sql.Timestamp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StructuredNetworkWordCountWindowed {
  def main(args: Array[String]): Unit = {

    val windowSize = 20
    val slideSize = 5
    val windowDuration = s"$windowSize seconds"
    val slideDuration = s"$slideSize seconds"

    val conf = new SparkConf().setAppName("StructuredNetworkWordCountWindowed").setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .option("includeTimestamp", true)
      .load()

    //    val words = lines.as[String].flatMap(_.split(" "))
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2)))
      .toDF("word", "timestamp")

    //    val wordCounts = words.groupBy("value").count()
    val windowedCounts = words.groupBy(
      window($"timestamp", windowDuration, slideDuration), $"word"
    ).count().orderBy("window")

    val query = windowedCounts.writeStream
      .outputMode("complete")
      //      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination()
  }
}
