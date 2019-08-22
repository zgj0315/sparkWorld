package org.after90.everisk

/**
  * Created by zhaogj on 15/08/2019.
  * spark-submit --jars fastjson-1.2.59.jar --class org.after90.everisk.ThreatData sparkworld_2.11-0.1.0.jar master messagePath outputPath
  */

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MessageStartData {

  case class Udid(udid: String)

  def main(args: Array[String]): Unit = {
    //    getUdidV4()
    //    getUdidV3()
    //    getUdidJF()
    doStat()
  }

  def getUdidV4(): Unit = {
    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/start/")

    val eV4DF = eV4File
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(x => {
        var body = x.getJSONObject("body")
        //        body.put("server_time", x.getLong("server_time"))
        body
      })
      .filter(_.containsKey("udid"))
      .map(x => {
        Udid(x.getString("udid"))
      })
      .toDF()
    eV4DF.createOrReplaceTempView("ev4_log")
    val sqlDF = spark.sql("SELECT distinct udid FROM ev4_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/udid")
    spark.stop()
  }


  def getUdidV3(): Unit = {
    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eFileV3 = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_start")
    val eV3DF = eFileV3
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => Udid(x(0).trim))
      .toDF()
    eV3DF.createOrReplaceTempView("ev3_log")
    val sqlDF = spark.sql("SELECT distinct udid FROM ev3_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/udid")
    spark.stop()
  }

  def getUdidJF(): Unit = {
    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/*.txt")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => Udid(x(0).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")
    val sqlDF = spark.sql("SELECT distinct udid FROM jf_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/udid")
    spark.stop()
  }

  def doStat(): Unit = {

    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/udid/part*")
    //    val eV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/aliyun/udid")


    val eV4DF = eV4File
      .map(x => {
        Udid(x)
      })
      .toDF()
    eV4DF.createOrReplaceTempView("ev4_log")

    val eFileV3 = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/udid/part*")
    val eV3DF = eFileV3
      .map(x => Udid(x))
      .toDF()
    eV3DF.createOrReplaceTempView("ev3_log")

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/udid/part*")
    val jfDF = jfFile
      .map(x => Udid(x))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")

    //v3设备个数
    var sqlDF = spark.sql("SELECT count(distinct udid) FROM ev3_log")
    //    sqlDF.show()
    //    699900

    //v4设备个数
    sqlDF = spark.sql("SELECT count(distinct udid) FROM ev4_log")
    //    sqlDF.show()
    //    1847206
    //    aliyun
    //    2396323

    //计费数据设备个数
    sqlDF = spark.sql("SELECT count(distinct udid) FROM jf_log")
    //    sqlDF.show()
    //    1142530

    //v3 v4设备交集
    sqlDF = spark.sql("SELECT count(distinct a.udid) FROM ev3_log as a, ev4_log as b where a.udid = b.udid")
    //    sqlDF.show()
    //    7064

    //v3 v4设备合集
    sqlDF = spark.sql("SELECT count(distinct udid) FROM (select * from ev3_log union select * from ev4_log) as e_log")
    //    sqlDF.show()
    //    2540042
    //    aliyun
    //    3095706

    //v3交集
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM ev3_log as a, jf_log as b where a.udid = b.udid")
    //    sqlDF.show()
    //    5548

    //v4交集
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM ev4_log as a, jf_log as b where a.udid = b.udid")
    sqlDF.show()
    //    8367

    //v3+v4交集的设备个数
    sqlDF = spark.sql("SELECT count(distinct b.udid) FROM (select * from ev3_log union select * from ev4_log) as a, jf_log as b where a.udid = b.udid")
    //    sqlDF.show()
    //    13771 1.2%

    spark.stop()
  }
}
