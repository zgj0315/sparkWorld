package com.bangcle.everisk

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Message {

  case class Message(agent_id: String, udid: String, start_id: String, msg_type: String, msg: String)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("MessageData")
      .setMaster("local[*]")
      //忽略掉非法压缩的文件，防止程序异常退出
      .setExecutorEnv("spark.sql.files.ignoreCorruptFiles", "false")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val msgFile = spark.sparkContext.textFile("/Volumes/SanDisk500G/message/201912/*20191212*.gz")
    val msgDF = msgFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => (x(0).trim, JSON.parseObject(x(2).trim)))
      .filter(_._2.containsKey("body"))
      .map(x => (x._1, x._2.getJSONObject("body")))
      .map(f = x => {
        var list = ArrayBuffer[Message]()
        list += Message(x._2.getString("agent_id"), x._2.getString("udid"), x._2.getString("start_id"),
          "ip_src", x._1)
        if (x._2.containsKey("wifi_info")) {
          val listWifi = x._2.getJSONArray("wifi_info")
          for (i <- 0 until listWifi.size()) {
            val wifi = listWifi.getJSONObject(i)
            list += Message(x._2.getString("agent_id"), x._2.getString("udid"), x._2.getString("start_id"),
              "wifi", wifi.getString("ssid") + ":" + wifi.getString("bssid"))
          }
        }
        list
      })
      .flatMap(x => x)
      .toDF()
    msgDF.createOrReplaceTempView("msg")
    var sqlDF = spark.sql("SELECT COUNT(*) FROM msg")
    sqlDF.show()
    //    var sqlDF = spark.sql("SELECT * FROM msg")
    //    sqlDF.show(1000)

    spark.stop()
  }
}
