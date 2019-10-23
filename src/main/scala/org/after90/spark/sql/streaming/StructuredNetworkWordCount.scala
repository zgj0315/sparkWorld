package org.after90.spark.sql.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object StructuredNetworkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StructuredNetworkWordCount").setMaster("local[*]")

    val spark = SparkSession
      .builder
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", "9999")
      .load()

    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      //      .outputMode("complete")
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
