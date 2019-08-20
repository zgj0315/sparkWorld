package org.after90.everisk

/**
  * Created by zhaogj on 15/08/2019.
  * spark-submit --jars fastjson-1.2.59.jar --class org.after90.everisk.ThreatData sparkworld_2.11-0.1.0.jar master messagePath outputPath
  */

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MessageStartData {

  case class ELog(udid: String, time: String)

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  def main(args: Array[String]): Unit = {
    var master = "local[*]"
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

    val eV4File = spark.sparkContext.textFile(messagePath)

    val eV4DF = eV4File
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(x => {
        var body = x.getJSONObject("body")
        body.put("server_time", x.getLong("server_time"))
        body
      })
      .filter(_.containsKey("udid"))
      .map(x => {
        ELog(x.getString("udid"), x.getString("server_time"))
      })
      .toDF()
    eV4DF.createOrReplaceTempView("ev4_log")

    val eFileV3 = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/everisk/v3.x_start/")
    val eV3DF = eFileV3
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => ELog(x(0).trim, x(1).trim))
      .toDF()
    eV3DF.createOrReplaceTempView("ev3_log")

    val jfFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/20190809/sb/")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")

    //v3设备个数
    var sqlDF = spark.sql("SELECT count(distinct udid) FROM ev3_log")
    //    sqlDF.show()
    //    699900
    //v4设备个数
    sqlDF = spark.sql("SELECT count(distinct udid) FROM ev4_log")
    //    sqlDF.show()
    //    1039024
    //计费数据设备个数
    sqlDF = spark.sql("SELECT count(distinct udid) FROM jf_log where udid <> '\\\\N' and udid <> 'null'")
    //    sqlDF.show()
    //    1142530

    //v3 v4设备交集
    sqlDF = spark.sql("SELECT count(distinct a.udid) FROM ev3_log as a, ev4_log as b where a.udid = b.udid")
    //    sqlDF.show()
    //    3426
    //v3 v4设备合集
    sqlDF = spark.sql("SELECT count(distinct udid) FROM (select * from ev3_log union select * from ev4_log) as e_log")
    //    sqlDF.show()
    //1735498
    //v3交集
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM ev3_log as a, jf_log as b where b.udid <> '\\\\N' and b.udid <> 'null' and a.udid = b.udid")
    //    sqlDF.show()
    //    5548
    //v4交集
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM ev4_log as a, jf_log as b where b.udid <> '\\\\N' and b.udid <> 'null' and a.udid = b.udid")
    sqlDF.show()
    //    6262
    //v3+v4交集的设备个数
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM (select * from ev3_log union select * from ev4_log) as a, jf_log as b where b.udid <> '\\\\N' and b.udid <> 'null' and a.udid = b.udid")
    //    sqlDF.show()
    //    11724
    spark.stop()
  }
}
