package org.after90.bangcle

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Imsi {

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  case class DevinfoLog(udid: String, imsi: String)

  case class Imsi(imsi: String)

  case class Udid(udid: String)

  /**
    * 获取计费数据imsi
    */
  def getImsiJF(): Unit = {
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
    val sqlDF = spark.sql("SELECT distinct imsi FROM jf_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/imsi")
    spark.stop()
  }

  /**
    * 获取威胁感知imei v3
    */
  def getImsiV3(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._

    val devinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_devinfo/")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      .map(x => DevinfoLog(x(4).trim, x(25).trim))
      .toDF()
    devinfoDF.createOrReplaceTempView("devinfo_log")

    val sqlDF = spark.sql("SELECT distinct imsi FROM devinfo_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/imsi")
    spark.stop()
  }

  /**
    * 获取威胁感知imei v4
    */
  def getImsiV4(): Unit = {
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
      .filter(_.containsKey("imsi"))
      .map(_.getString("imsi"))
      .map(_.split(","))
      .flatMap(x => x)
      .map(_.replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""))
      .map(Imsi(_))
      .toDF()
    devinfoDF.createOrReplaceTempView("imsi_v4")

    val sqlDF = spark.sql("SELECT distinct imsi FROM imsi_v4")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/imsi")
    spark.stop()
  }

  def imsiStat(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")

    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._
    val imeiV3File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/imsi/part*")
    val imeiV3DF = imeiV3File
      .map(Imsi(_))
      .toDF()
    imeiV3DF.createOrReplaceTempView("imsi_v3")
    var sqlDF = spark.sql("select count(imsi) from imsi_v3")
    //    sqlDF.show()
    //    559325

    val imeiV4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/imsi/part*")
    val imeiV4DF = imeiV4File
      .map(Imsi(_))
      .toDF()
    imeiV4DF.createOrReplaceTempView("imsi_v4")
    sqlDF = spark.sql("select count(imsi) from imsi_v4")
    //    sqlDF.show()
    //    2059180

    val imeiJFFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/imsi/part*")
    val imeiJFDF = imeiJFFile
      .map(Imsi(_))
      .toDF()
    imeiJFDF.createOrReplaceTempView("imsi_jf")
    sqlDF = spark.sql("select count(imsi) from imsi_jf")
    //    sqlDF.show()
    //    158596

    sqlDF = spark.sql("select count(a.imsi) from imsi_jf as a, imsi_v3 as b where a.imsi = b.imsi")
    //    sqlDF.show()
    //    4868 3%
    sqlDF = spark.sql("select count(a.imsi) from imsi_jf as a, imsi_v4 as b where a.imsi = b.imsi")
    //    sqlDF.show()
    //    10565 6.7%

    sqlDF = spark.sql("select count(a.imsi) from imsi_jf as a, (select * from imsi_v3 union select * from imsi_v4) as b where a.imsi = b.imsi")
    //    sqlDF.show()
    //    15215 9.6%
    spark.stop()
  }
}
