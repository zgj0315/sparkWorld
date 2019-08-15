package org.after90.southbase

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EveriskData {

  case class StartLog(udid: String, time: String)

  case class DevinfoLog(udid: String, imei: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EveriskData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    //启动数据
    val startFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/everisk/v3.x_start/")
    val startDF = startFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => StartLog(x(0).trim, x(1).trim))
      .toDF()
    startDF.createOrReplaceTempView("start_log")

    //设备数据
    val devinfoFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/everisk/v3.x_devinfo/")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      //.filter(_.size == 2)
      .map(x => DevinfoLog(x(4).trim, x(18).trim))
      .toDF()
    devinfoDF.createOrReplaceTempView("devinfo_log")

    var sqlDF = spark.sql("SELECT * FROM devinfo_log")
    sqlDF.show()

    spark.stop()
  }
}
