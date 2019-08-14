package org.after90.southbase

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EveriskData {

  case class ELog(udid: String, time: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("EveriskData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/everisk/v3.x_start/")
    val eDF = eFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => ELog(x(0).trim, x(1).trim))
      .toDF()
    eDF.createOrReplaceTempView("e_log")

    //数据条数
    var sqlDF = spark.sql("SELECT count(*) FROM e_log")
    //    sqlDF.show()
    //1565897
    //设备个数
    sqlDF = spark.sql("SELECT count(distinct udid) FROM e_log")
    sqlDF.show()
    //699900

    spark.stop()
  }
}
