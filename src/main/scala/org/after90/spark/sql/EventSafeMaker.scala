package org.after90.spark.sql

import org.apache.spark.sql.SparkSession

object EventSafeMaker {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("EventSafeMaker")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df = List(
      ("012229", 1234567811, 1),
      ("012229", 1234567812, 2),
      ("012229", 1234567813, 1),
      ("012229", 1234567815, 1),
      ("012229", 1234567818, 1),
      ("012229", 1234567819, 1),
      ("012229", 1234567820, 1),
      ("012230", 1234567891, 1),
      ("012230", 1234567892, 1)
    ).toDF("udid", "time", "count")
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val wSpec = Window.partitionBy("udid")
      .orderBy("time")
      //.rowsBetween(-1, 1)
      .rangeBetween(-2, 0) //三秒

    df.withColumn("movingCount",
      sum(df("count")).over(wSpec)).show()

    Thread.sleep(1000 * 60 * 60)
    spark.close()

  }
}
