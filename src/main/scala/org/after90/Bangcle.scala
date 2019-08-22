package org.after90

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Bangcle {

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  case class DevinfoLog(udid: String, imei: String)

  case class Imei(imei: String)

  case class Udid(udid: String)

  def main(args: Array[String]): Unit = {
    //    getImeiJF()
    //    getImeiV3()
    //    getImeiV4()
    imeiStat()
    //    getUdidV4()
    //    getUdidV3()
    //    getUdidJF()
    //    udidStat()
  }

  /**
    * 获取计费数据imei
    */
  def getImeiJF(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/*.txt")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")
    val sqlDF = spark.sql("SELECT distinct imei FROM jf_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/imei")
    spark.stop()
  }

  /**
    * 获取威胁感知imei v3
    */
  def getImeiV3(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val devinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_devinfo/")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      //.filter(_.size == 2)
      .map(x => DevinfoLog(x(4).trim, x(18).trim))
      .toDF()
    devinfoDF.createOrReplaceTempView("devinfo_log")

    val sqlDF = spark.sql("SELECT distinct imei FROM devinfo_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/imei")
    spark.stop()
  }

  /**
    * 获取威胁感知imei v4
    */
  def getImeiV4(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val devinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/devinfo/*.txt")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      //      .filter(_.containsKey("udid"))
      .filter(_.containsKey("imei"))
      .map(_.getString("imei"))
      .map(_.split(","))
      .flatMap(x => x)
      .map(_.replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""))
      .map(Imei(_))
      .toDF()
    devinfoDF.createOrReplaceTempView("imei_v4")

    val sqlDF = spark.sql("SELECT distinct imei FROM imei_v4")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/imei")
    spark.stop()
  }

  def imeiStat(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._
    val imeiV3File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/imei/part*")
    val imeiV3DF = imeiV3File
      .map(Imei(_))
      .toDF()
    imeiV3DF.createOrReplaceTempView("imei_v3")
    var sqlDF = spark.sql("select count(imei) from imei_v3")
    //    sqlDF.show()
    //    846079

    val imeiV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/imei/part*")
    val imeiV4DF = imeiV4File
      .map(Imei(_))
      .toDF()
    imeiV4DF.createOrReplaceTempView("imei_v4")
    sqlDF = spark.sql("select count(imei) from imei_v4")
    //    sqlDF.show()
    //    3624111

    val imeiJFFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/imei/part*")
    val imeiJFDF = imeiJFFile
      .map(Imei(_))
      .toDF()
    imeiJFDF.createOrReplaceTempView("imei_jf")
    sqlDF = spark.sql("select count(imei) from imei_jf")
    //    sqlDF.show()
    //    168500

    sqlDF = spark.sql("select count(a.imei) from imei_jf as a, imei_v3 as b where a.imei = b.imei")
    //    sqlDF.show()
    //    4965 2.95%
    sqlDF = spark.sql("select count(a.imei) from imei_jf as a, imei_v4 as b where a.imei = b.imei")
    //    sqlDF.show()
    //    10645 6.3%

    sqlDF = spark.sql("select count(a.imei) from imei_jf as a, (select * from imei_v3 union select * from imei_v4) as b where a.imei = b.imei")
    sqlDF.show()
    //    15379 9.13%
    spark.stop()
  }

  def getUdidV3(): Unit = {
    val conf = new SparkConf().setAppName("ThreatData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eStartFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_start")
    val eStartDF = eStartFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => Udid(x(0).trim))
      .toDF()
    eStartDF.createOrReplaceTempView("start")
    val eDevinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_devinfo")
    val eDevinfoDF = eDevinfoFile
      .map(_.split("/t"))
      .filter(_.size >= 4)
      .map(x => Udid(x(4).trim))
      .toDF()
    eDevinfoDF.createOrReplaceTempView("devinfo")

    val sqlDF = spark.sql("SELECT distinct udid FROM (select udid from start union select udid from devinfo) as tbl_udid")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/udid")
    spark.stop()
  }

  def getUdidV4(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val eStartFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/start/")
    val eStartDF = eStartFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .map(x => {
        Udid(x.getString("udid"))
      })
      .toDF()
    eStartDF.createOrReplaceTempView("start")

    val eDevinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/start/")
    val eDevinfoDF = eDevinfoFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .map(x => {
        Udid(x.getString("udid"))
      })
      .toDF()
    eDevinfoDF.createOrReplaceTempView("devinfo")
    val sqlDF = spark.sql("SELECT distinct udid FROM (select udid from start union select udid from devinfo) as tbl_udid")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/udid")
    spark.stop()
  }

  def getUdidJF(): Unit = {
    val conf = new SparkConf().setAppName("JiFeiData").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/*.txt")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")
    val sqlDF = spark.sql("SELECT distinct udid FROM jf_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/udid")
  }


  def udidStat(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val udidV3File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/udid/part*")
    val udidV3DF = udidV3File
      .map(Udid(_))
      .toDF()
    udidV3DF.createOrReplaceTempView("udid_v3")
    var sqlDF = spark.sql("select count(udid) from udid_v3")
    //    sqlDF.show()
    //v3 设备数量
    //    702057

    val udidV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/udid/part*")
    val udidV4DF = udidV4File
      .map(Udid(_))
      .toDF()
    udidV4DF.createOrReplaceTempView("udid_v4")
    sqlDF = spark.sql("select count(udid) from udid_v4")
    //    sqlDF.show()
    //v4 设备数量
    //    1847206

    val udidJFFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/udid/part*")
    val udidJFDF = udidJFFile
      .map(Udid(_))
      .toDF()
    udidJFDF.createOrReplaceTempView("udid_jf")
    sqlDF = spark.sql("select count(udid) from udid_jf")
    //    sqlDF.show()
    //计费设备数量
    //    1142532

    sqlDF = spark.sql("select count(a.udid) from udid_jf as a, udid_v3 as b where a.udid = b.udid")
    //    sqlDF.show()
    //计费与v3的交集
    //    5548

    sqlDF = spark.sql("select count(a.udid) from udid_jf as a, udid_v4 as b where a.udid = b.udid")
    //    sqlDF.show()
    //计费与v4的交集
    //    8367

    sqlDF = spark.sql("select count(a.udid) from udid_jf as a, (select * from udid_v3 union select * from udid_v4) as b where a.udid = b.udid")
    sqlDF.show()
    //计费与v3+v4的交集
    //    13771 1.21%
    spark.stop()
  }
}
