package org.after90.southbase

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JoinData {

  case class ELog(udid: String, time: String)

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JoinData").setMaster("local[*]")

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

    val jfFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/sb/")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")
    //数据条数
    var sqlDF = spark.sql("SELECT count(*) FROM e_log as a, jf_log as b where b.udid <> '\\\\N' and b.udid <> 'null' and a.udid = b.udid")
    //    sqlDF.show()
    //256346
    //设备个数
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM e_log as a, jf_log as b where b.udid <> '\\\\N' and b.udid <> 'null' and a.udid = b.udid")
    sqlDF.show()
    //5548
    spark.stop()
  }
}
