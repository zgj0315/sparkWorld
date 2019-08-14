package org.after90.southbase

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JiFeiData {

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JiFeiData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val jfFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/sb/")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")

    val sqlFilter = "udid <> '\\\\N' and udid <> 'null'"
    //sql
    var sqlDF = spark.sql("SELECT * FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //数据条数
    //    sqlDF = spark.sql("SELECT count(*) FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //1212690
    //设备个数
    //    sqlDF = spark.sql("SELECT count(distinct udid) FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //1142530
    //imei个数
    //    sqlDF = spark.sql("SELECT count(distinct imei) FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //165050
    //    sqlDF = spark.sql("SELECT * FROM jf_log where " + sqlFilter)
    //    sqlDF.groupBy("imei").count().orderBy($"count".desc).show()
    //165531
    //imsi个数
    //    sqlDF = spark.sql("SELECT count(distinct imsi) FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //155245
    //udid+imei个数
    //    sqlDF = spark.sql("SELECT count(concat(udid, imei)) FROM jf_log where " + sqlFilter)
    //    sqlDF.show()
    //1212690
    //udid+imei去重个数
    sqlDF = spark.sql("SELECT count(distinct concat(udid, imei)) FROM jf_log where " + sqlFilter)
    sqlDF.show()
    //1142554

    spark.stop()
  }
}
