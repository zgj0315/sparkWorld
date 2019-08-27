package org.after90.bangcle

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UdidImei {

  case class JFLog(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  case class DevinfoLog(udid: String, imei: String)

  /**
    * 3.0版本数据
    */
  def v3(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val devinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/v3.0_devinfo/")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      .map(x => DevinfoLog(x(4).trim, x(18).trim))
      .toDF()
    devinfoDF.createOrReplaceTempView("devinfo_log")

    val sqlDF = spark.sql("SELECT distinct udid, imei FROM devinfo_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/udid_imei")
    spark.stop()
  }

  /**
    * 4.0版本数据
    */
  def v4(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val devinfoFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/devinfo/*.txt")
    val devinfoDF = devinfoFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .filter(_.containsKey("imei"))
      .map(x => DevinfoLog(x.getString("udid"), x.getString("imei")))
      .flatMap(x => {
        val imeis = x.imei.split(",")
        val arr = new Array[DevinfoLog](imeis.size)
        for (i <- 0 until imeis.size) {
          arr(i) = DevinfoLog(x.udid, imeis(i).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""))
        }
        arr
      })
      .toDF()
    devinfoDF.createOrReplaceTempView("imei_v4")

    val sqlDF = spark.sql("SELECT distinct udid, imei FROM imei_v4")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/udid_imei")
    spark.stop()
  }

  /**
    * 计费数据
    */
  def jf(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/*.txt")
    val jfDF = jfFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFLog(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jfDF.createOrReplaceTempView("jf_log")
    val sqlDF = spark.sql("SELECT distinct udid, imei FROM jf_log")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/udid_imei")
    spark.stop()
  }

  /**
    * 分析udid和imei关系
    */
  def stat(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val v3File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/udid_imei/part*")
    val v3DF = v3File
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => DevinfoLog(x(0), x(1)))
      .filter(_.udid.length == 36) //数据清洗
      .filter(_.imei.length == 15) //数据清洗
      .toDF()
    v3DF.createOrReplaceTempView("udid_imei_v3")
    // v3.0数据条数
    var sqlDF = spark.sql("select count(*) from udid_imei_v3")
    //    sqlDF.show()
    //    888197

    val v4File = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/udid_imei/part*")
    val v4DF = v4File
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => DevinfoLog(x(0), x(1)))
      .filter(_.imei.length == 15) //数据清洗
      .toDF()
    v4DF.createOrReplaceTempView("udid_imei_v4")
    // v4.3数据条数
    sqlDF = spark.sql("select count(*) from udid_imei_v4")
    //    sqlDF.show()
    //    3633839

    val jfFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/udid_imei/part*")
    val jfDF = jfFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => DevinfoLog(x(0), x(1)))
      .filter(_.udid.length == 36) //数据清洗
      .filter(_.imei.length == 15) //数据清洗
      .toDF()
    jfDF.createOrReplaceTempView("udid_imei_jf")

    //数据清洗
    //计费数据
    sqlDF = spark.sql("select length(udid) from udid_imei_jf")
    //    sqlDF.groupBy("length(udid)").count().show()
    sqlDF = spark.sql("select length(imei) from udid_imei_jf")
    //    sqlDF.groupBy("length(imei)").count().show()
    sqlDF = spark.sql("select length(udid) from udid_imei_v3")
    //    sqlDF.groupBy("length(udid)").count().show()
    sqlDF = spark.sql("select length(imei) from udid_imei_v3")
    //    sqlDF.groupBy("length(imei)").count().show()
    sqlDF = spark.sql("select length(udid) from udid_imei_v4")
    //    sqlDF.groupBy("length(udid)").count().show()
    sqlDF = spark.sql("select length(imei) from udid_imei_v4")
    //    sqlDF.groupBy("length(imei)").count().show()

    //数据分析
    sqlDF = spark.sql("select * from udid_imei_jf")
    //    sqlDF.groupBy("udid").count().sort($"count".desc).show()
    sqlDF = spark.sql("select * from udid_imei_jf")
    //    sqlDF.groupBy("imei").count().sort($"count".desc).show()
    //    sqlDF.groupBy("imei").count().sort($"count".desc).repartition(1)
    //      .write.format("csv").save("/Volumes/HDD01/bangcle/jifei/imei_udid")

    //    计费数据
    //一个imei上，存在几百个udid的较多
    sqlDF = spark.sql("select imei, count(imei) from udid_imei_jf group by imei having count(imei) > 1")
    //    sqlDF.show()
    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_jf group by imei) as a")
    //    sqlDF.show()
    //    imei个数
    //    164799

    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_jf group by imei having count(imei) > 1) as a")
    //    sqlDF.show()
    //    imei对应单一设备
    //    105657 64.1%

    //    v3数据
    //一个imei上，存在几百个udid的较多
    sqlDF = spark.sql("select imei, count(imei) from udid_imei_v3 group by imei having count(imei) > 1")
    //    sqlDF.show()

    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_v3 group by imei) as a")
    //    sqlDF.show()
    //    imei个数
    //    795023

    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_v3 group by imei having count(imei) > 1) as a")
    //    sqlDF.show()
    //    imei对应单一设备
    //    5578 0.7%

    //    v4数据
    //一个imei上，存在几百个udid的较多
    sqlDF = spark.sql("select imei, count(imei) from udid_imei_v4 group by imei having count(imei) > 1")
    //    sqlDF.show()

    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_v4 group by imei) as a")
    //    sqlDF.show()
    //    imei个数
    //    3623765

    sqlDF = spark.sql("select count(imei) from (select imei, count(imei) from udid_imei_v4 group by imei having count(imei) > 1) as a")
    //    sqlDF.show()
    //    imei对应单一设备
    //    790 0.02%

    //分析udid和imei对应关系

    //    计费数据
    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_jf group by udid) as a")
    //    sqlDF.show()
    //    udid个数
    //    1141285

    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_jf group by udid having count(udid) > 2) as a")
    //    sqlDF.show()
    //    udid对应两个imei
    //    1

    //    v3数据
    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_v3 group by udid) as a")
    //    sqlDF.show()
    //    udid个数
    //    806355

    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_v3 group by udid having count(udid) > 2) as a")
    //    sqlDF.show()
    //    udid对应两个imei
    //    22

    //    v4数据
    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_v4 group by udid) as a")
    //    sqlDF.show()
    //    udid个数
    //    1839579

    sqlDF = spark.sql("select count(udid) from (select udid, count(udid) from udid_imei_v4 group by udid having count(udid) > 2) as a")
    //    sqlDF.show()
    //    udid对应两个imei
    //    158

    sqlDF = spark.sql("select * from udid_imei_v4")
    //    sqlDF.groupBy("udid").count().sort($"udid".desc).show()

    // 计费数据条数
    sqlDF = spark.sql("select count(*) from udid_imei_jf")
    //    sqlDF.show()
    //    1146631
    //    1141301

    // v3设备个数
    sqlDF = spark.sql("select count(distinct udid) from udid_imei_v3")
    //    sqlDF.show()
    //    883957
    //    806355

    // v4设备个数
    sqlDF = spark.sql("select count(distinct udid) from udid_imei_v4")
    //    sqlDF.show()
    //    1846021
    //    1839579

    // v3+v4设备个数
    sqlDF = spark.sql("select count(distinct a.udid) from (select * from udid_imei_v3 union select * from udid_imei_v4) as a")
    //    sqlDF.show()
    //    2723888
    //    2640179

    // 计费设备个数
    sqlDF = spark.sql("select count(distinct udid) from udid_imei_jf")
    //    sqlDF.show()
    //    1142532
    //    1141285

    // v3与计费udid匹配
    sqlDF = spark.sql("select count(distinct a.udid) from udid_imei_v3 as a, udid_imei_jf as b where a.udid = b.udid")
    //    sqlDF.show()
    //    2908 0.25%
    //    2811


    // v4与计费udid匹配
    sqlDF = spark.sql("select count(distinct a.udid) from udid_imei_v4 as a, udid_imei_jf as b where a.udid = b.udid")
    //    sqlDF.show()
    //    8347 0.73%
    //    8201

    // v3+v4与计费udid匹配
    sqlDF = spark.sql("select count(distinct a.udid) from (select * from udid_imei_v3 union select * from udid_imei_v4) as a, udid_imei_jf as b where a.udid = b.udid")
    //    sqlDF.show()
    //    11126 0.97%
    //    10890

    // v3 imei个数
    sqlDF = spark.sql("select count(distinct imei) from udid_imei_v3")
    //    sqlDF.show()
    //    846079
    //    795023

    // v4 imei个数
    sqlDF = spark.sql("select count(distinct imei) from udid_imei_v4")
    //    sqlDF.show()
    //    3624111
    //    3623765

    // v3+v4 imei个数
    sqlDF = spark.sql("select count(distinct a.imei) from (select * from udid_imei_v3 union select * from udid_imei_v4) as a")
    //    sqlDF.show()
    //    4461993
    //    4410598

    // 计费imei个数
    sqlDF = spark.sql("select count(distinct imei) from udid_imei_jf")
    //    sqlDF.show()
    //    168500
    //    164799


    // v3与计费imei匹配
    sqlDF = spark.sql("select count(distinct a.imei) from udid_imei_v3 as a, udid_imei_jf as b where a.imei = b.imei")
    //    sqlDF.show()
    //    4965 2.95%
    //    4604

    // v4与计费imei匹配
    sqlDF = spark.sql("select count(distinct a.imei) from udid_imei_v4 as a, udid_imei_jf as b where a.imei = b.imei")
    //    sqlDF.show()
    //    10645 6.31%
    //    10618

    // v3+v4与计费imei匹配
    sqlDF = spark.sql("select count(distinct a.imei) from (select * from udid_imei_v3 union select * from udid_imei_v4) as a, udid_imei_jf as b where a.imei = b.imei")
    //    sqlDF.show()
    //    15379 9.13%
    //    15003

    spark.stop()
  }
}
