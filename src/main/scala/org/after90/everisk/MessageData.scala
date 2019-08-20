package org.after90.everisk

/**
  * Created by zhaogj on 15/08/2019.
  * spark-submit --jars fastjson-1.2.59.jar --class org.after90.everisk.ThreatData sparkworld_2.11-0.1.0.jar master messagePath outputPath
  */

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MessageData {

  case class ELog(udid: String, imeiA: String, imeiB: String, time: Long)

  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    //    var messagePath = "/Users/zhaogj/bangcle/everisk/src/message_data_20190805_1564969723942_1565231757484"
    var messagePath = "/Users/zhaogj/bangcle/southbase/20190809/everisk/v4.3/2019_0819_start.txt"
    var outputPath = "/Users/zhaogj/bangcle/southbase/20190809/everisk/v4.3/output"

    if (args.length == 3) {
      master = args(0)
      messagePath = args(1)
      outputPath = args(2)
    }

    val conf = new SparkConf().setAppName("ThreatData").setMaster(master)

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eFile = spark.sparkContext.textFile(messagePath)

    val eDF = eFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(x => {
        var body = x.getJSONObject("body")
        body.put("server_time", x.getLong("server_time"))
        body
      })
      .filter(_.containsKey("udid"))
      //      .filter(_.containsKey("imei"))
      //      .filter(_.containsKey("server_time"))
      //      .filter(_.getLong("server_time") >= 1564969724122L)
      //      .filter(_.getLong("server_time") < 1564999724122L)
      .map(x => {
      val imeis = x.getString("imei").split(",")
      var imeiA = imeis(0).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "")
      var imeiB = ""
      if (imeis.size == 2) {
        imeiB = imeis(1).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", "")
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

    var sqlDF = spark.sql("SELECT distinct udid, imeiA, imeiB FROM e_log")
    //    sqlDF.show()
    sqlDF.write.format("csv").save(outputPath)
    spark.stop()
  }
}
