package org.after90.spark.sql

import org.apache.spark.sql.SparkSession

object SMATest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("SMAtest")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val df = List(
      ("站点1", "2017-01-01", 50),
      ("站点1", "2017-01-02", 45),
      ("站点1", "2017-01-03", 55),
      ("站点2", "2017-01-01", 25),
      ("站点2", "2017-01-02", 29),
      ("站点2", "2017-01-03", 27)
    ).toDF("site", "date", "user_cnt")
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._
    val wSpec = Window.partitionBy("site")
      .orderBy("date")
      .rowsBetween(-1, 1)

    df.withColumn("movingAvg",
      avg(df("user_cnt")).over(wSpec)).show()

    df.withColumn("movingCount",
      count(df("*")).over(wSpec)).show()

    spark.close()

  }
}
