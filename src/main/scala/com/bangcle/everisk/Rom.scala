package com.bangcle.everisk

import com.alibaba.fastjson.JSON
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object Rom {

  // scp target/scala-2.12/sparkworld_2.12-0.1.0.jar zhaogj@192.168.31.120:~/apk/lib/
  //  case class Message(agent_id: String, udid: String, start_id: String, msg_type: String, msg: String)

  case class Rom(agent_id: String, udid: String, arm_art: String, arm_oat: String, arm64_art: String,
                 arm64_oat: String)

  def main(args: Array[String]): Unit = {
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val conf = new SparkConf()
      .setAppName("Rom")
      .setMaster("local[*]")
      //忽略掉非法压缩的文件，防止程序异常退出
      .setExecutorEnv("spark.sql.files.ignoreCorruptFiles", "true")
      .setExecutorEnv("spark.sql.files.ignoreMissingFiles", "true")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    //    val msgFile = spark.sparkContext.textFile("/Users/zhaogj/tmp/message")
    val msgFile = spark.sparkContext.textFile("/home/zhaogj/message/*/*.gz")
    val msgDF = msgFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => (x(0).trim, JSON.parseObject(x(2).trim)))
      .filter(_._2.containsKey("body"))
      .map(x => (x._1, x._2.getJSONObject("body")))
      .filter(x => {
        "start_info".equals(x._2.getString("protol_type"))
      })
      .map(f = x => {
        var list = ArrayBuffer[Rom]();
        val agent_id = x._2.getString("agent_id")
        val udid = x._2.getString("udid")
        val data = x._2.getJSONArray("data")

        for (i <- 0 until data.size()) {
          val json = data.getJSONObject(i)
          if ("rom_file".equals(json.getString("protol_type"))) {
            val rom_file = json.getJSONObject("rom_file")
            list += Rom(agent_id, udid, rom_file.getString("/system/framework/arm/boot.art"),
              rom_file.getString("/system/framework/arm/boot.oat"),
              rom_file.getString("/system/framework/arm64/boot.oat"),
              rom_file.getString("/system/framework/arm64/boot.art"))
          }
        }
        list
      }
      )
      .flatMap(x => x)
      .toDF()

    msgDF.createOrReplaceTempView("rom")
    val sqlDF = spark.sql("SELECT DISTINCT * FROM rom")
    //    sqlDF.show(1000)
    sqlDF
      .repartition(1)
      .write
      .option("delimiter", "\t")
      .csv("/home/zhaogj/job/rom/result")

    spark.stop()
  }
}
