package com.bangcle.everisk

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Message {

  //  case class Message(agent_id: String, udid: String, start_id: String, msg_type: String, msg: String)

  case class Apk(agent_id: String, udid: String, pkg_name: String, app_name: String, ver_name: String,
                 cert_md5: String, ver_code: String, cert_issure: String, md5: String)

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

    val msgFile = spark.sparkContext.textFile("/Volumes/SanDisk500G/message/*/*.gz")
    val msgDF = msgFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => (x(0).trim, JSON.parseObject(x(2).trim)))
      .filter(_._2.containsKey("body"))
      .map(x => (x._1, x._2.getJSONObject("body")))
      .filter(x => {
        x._2.containsKey("protol_type") &&
          x._2.containsKey("apk_flag") &&
          x._2.containsKey("data") &&
          x._2.getString("protol_type").equals("apkinfo") &&
          (x._2.getString("apk_flag").equals("all") ||
            x._2.getString("apk_flag").equals("update")
            )
      })
      .map(f = x => {
        var list = ArrayBuffer[Apk]()
        //        list += Apk(x._2.getString("agent_id"), x._2.getString("udid"), x._2.getString("start_id"),
        //          "ip_src", x._1)
        //        if (x._2.getString("apk_flag").equals("update") && x._2.containsKey("data")) {
        val agent_id = x._2.getString("agent_id");
        val udid = x._2.getString("udid");
        val data = x._2.getJSONArray("data");

        for (i <- 0 until data.size()) {
          val json = data.getJSONObject(i)
          var app_name = json.getString("app_name")
          if (app_name != null) {
            app_name = app_name
              .replace("\n", "")
              .replace("\t", "");
          }
          var cert_issure = json.getString("cert_issure");
          if (cert_issure != null) {
            cert_issure = cert_issure
              .replace("\n", "")
              .replace("\t", "");
          }
          list += Apk(agent_id, udid, json.getString("pkg_name"), app_name,
            json.getString("ver_name"), json.getString("cert_md5"), json.getString("ver_code"),
            cert_issure, json.getString("md5"))
        }

        //        }
        list
      })
      .flatMap(x => x)
      .toDF()
    msgDF.createOrReplaceTempView("apk")
    var sqlDF = spark.sql("SELECT distinct * FROM apk")
    //    sqlDF.show(1000)
    sqlDF
      .repartition(1)
      .write
      .option("delimiter", "\t")
      .csv("/Users/zhaogj/tmp/apk")

    //    var sqlDF = spark.sql("SELECT * FROM msg")
    //    sqlDF.show(1000)

    spark.stop()
  }
}
