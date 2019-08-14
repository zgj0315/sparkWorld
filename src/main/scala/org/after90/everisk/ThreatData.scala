package org.after90.everisk

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ThreatData {

  case class ELog(udid: String, imeiA: String, imeiB: String, time: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/everisk/src/threat_data_20190606_1559801814361_1559804403970")
    val eDF = eFile
      .map(_.split("\t"))
      .filter(_.size == 2)
      .map(x => JSON.parseObject(x(1).trim))
      .filter(_.containsKey("udid"))
      .filter(_.containsKey("imei"))
      .filter(_.containsKey("server_time"))
      .filter(_.getLong("server_time") >= 1559804055236L)
      .filter(_.getLong("server_time") < 1565020800000L)
      .map(x => {
        val imeis = x.getString("imei").split(",")
        var imeiA = imeis(0).replaceAll("\"", "")
        var imeiB = ""
        if (imeis.size == 2) {
          imeiB = imeis(1).replaceAll("\"", "")
        }
        //排序imei
        if (imeiA > imeiB) {
          val tmp = imeiA
          imeiA = imeiB
          imeiB = tmp
        }
        ELog(x.getString("udid"), imeiA, imeiB, x.getLong("server_time"))
      })
      .toDF()
    eDF.createOrReplaceTempView("e_log")

    var sqlDF = spark.sql("SELECT * FROM e_log")
    sqlDF.show()

    spark.stop()
  }
}
