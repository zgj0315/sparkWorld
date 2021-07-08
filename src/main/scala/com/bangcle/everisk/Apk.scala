package com.bangcle.everisk

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Apk {

  // scp target/scala-2.12/sparkworld_2.12-0.1.0.jar zhaogj@192.168.31.120:~/apk/lib/
  //  case class Message(agent_id: String, udid: String, start_id: String, msg_type: String, msg: String)

  case class Apk(agent_id: String, udid: String, pkg_name: String, app_name: String, ver_name: String,
                 cert_md5: String, ver_code: String, cert_issure: String, md5: String, install_time: Long,
                 update_time: Long, uninstall_time: Long)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Apk")
      .setMaster("local[*]")
      //忽略掉非法压缩的文件，防止程序异常退出
      .setExecutorEnv("spark.sql.files.ignoreCorruptFiles", "true")
      .setExecutorEnv("spark.sql.files.ignoreMissingFiles", "true")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val msgFile = spark.sparkContext.textFile("/home/zhaogj/message/*/*.gz")
    //    val msgFile = spark.sparkContext.textFile("/Users/zhaogj/tmp/message_data_20210513_1620889320913_1620889323909.gz")

    val msgDF = msgFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => (x(0).trim, JSON.parseObject(x(2).trim)))
      .filter(_._2.containsKey("body"))
      .map(x => (x._1, x._2.getJSONObject("body")))
      .filter(x => {
        "apkinfo".equals(x._2.getString("protol_type")) ||
          "multi_message".equals(x._2.getString("protol_type"))
      })
      .map(f = x => {
        var list = ArrayBuffer[Apk]();
        val protol_type = x._2.getString("protol_type")
        val agent_id = x._2.getString("agent_id")
        val udid = x._2.getString("udid")
        val data = x._2.getJSONArray("data")
        if ("apkinfo".equals(protol_type)) {
          if ("all".equals(x._2.getString("apk_flag")) || "update".equals(x._2.getString("apk_flag"))) {
            for (i <- 0 until data.size()) {
              val json = data.getJSONObject(i)
              var app_name = json.getString("app_name")
              if (app_name != null) {
                app_name = app_name
                  .replace("\n", "")
                  .replace("\t", "")
                  .replace("\\p{C}", "")
              }
              var cert_issure = json.getString("cert_issure");
              if (cert_issure != null) {
                cert_issure = cert_issure
                  .replace("\n", "")
                  .replace("\t", "")
                  .replace("\\p{C}", "")
              }
              var ver_name = json.getString("ver_name");
              if (ver_name != null) {
                ver_name = ver_name
                  .replace("\n", "")
                  .replace("\t", "")
                  .replace("\\p{C}", "")
              }
              val install_type = json.getString("type")
              val install_time = json.getLong("install_time");
              var update_time = 0L;
              var uninstall_time = 0L;
              if ("install".equals(install_type)) {
                //do nothing
              } else if ("reinstall".equals(install_type)) {
                update_time = json.getLong("update_time");
              } else if ("uninstall".equals(install_type)) {
                uninstall_time = json.getLong("update_time");
              }
              list += Apk(agent_id, udid, json.getString("pkg_name"), app_name,
                ver_name, json.getString("cert_md5"), json.getString("ver_code"),
                cert_issure, json.getString("md5"), install_time, update_time, uninstall_time)
            }
          }
        } else if ("multi_message".equals(protol_type)) {
          for (i <- 0 until data.size()) {
            val json = data.getJSONObject(i)
            if ("apkinfo".equals(json.getString("protol_type"))) {
              if ("all".equals(json.getString("apk_flag")) || "update".equals(json.getString("apk_flag"))) {
                val data_data = json.getJSONArray("data")
                for (j <- 0 until data_data.size()) {
                  val data_json = data_data.getJSONObject(j)

                  var app_name = data_json.getString("app_name")
                  if (app_name != null) {
                    app_name = app_name
                      .replace("\n", "")
                      .replace("\t", "")
                      .replace("\\p{C}", "")
                  }
                  var cert_issure = data_json.getString("cert_issure");
                  if (cert_issure != null) {
                    cert_issure = cert_issure
                      .replace("\n", "")
                      .replace("\t", "")
                      .replace("\\p{C}", "")
                  }
                  var ver_name = data_json.getString("ver_name");
                  if (ver_name != null) {
                    ver_name = ver_name
                      .replace("\n", "")
                      .replace("\t", "")
                      .replace("\\p{C}", "")
                  }
                  val install_type = json.getString("type")
                  val install_time = json.getLong("install_time");
                  var update_time = 0L;
                  var uninstall_time = 0L;
                  if ("install".equals(install_type)) {
                    //do nothing
                  } else if ("reinstall".equals(install_type)) {
                    update_time = json.getLong("update_time");
                  } else if ("uninstall".equals(install_type)) {
                    uninstall_time = json.getLong("update_time");
                  }
                  list += Apk(agent_id, udid, json.getString("pkg_name"), app_name,
                    ver_name, json.getString("cert_md5"), json.getString("ver_code"),
                    cert_issure, json.getString("md5"), install_time, update_time, uninstall_time)
                }
              }
            }
          }
        }
        list
      }
      )
      .flatMap(x => x)
      .toDF()

    msgDF.createOrReplaceTempView("apk")
    //    val sqlDF = spark.sql("SELECT DISTINCT * FROM apk")
    val sqlDF = spark.sql("SELECT agent_id, udid, pkg_name, app_name, ver_name, cert_md5, " +
      "ver_code, cert_issure, md5, max(install_time) as install_time, max(update_time) as update_time, " +
      "max(uninstall_time) as uninstall_time " +
      "FROM apk GROUP BY agent_id, udid, pkg_name, app_name, ver_name, cert_md5, ver_code, cert_issure, " +
      "md5")
    //    sqlDF.show()
    sqlDF
      .repartition(1)
      .write
      .option("delimiter", "\t")
      .csv("/home/zhaogj/apk/result")

    spark.stop()
  }
}
