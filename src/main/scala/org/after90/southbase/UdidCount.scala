package org.after90.southbase

/**
  * Created by zhaogj on 30/12/2016.
  * spark-submit --master yarn-cluster --class org.after90.spark.WordCount sbtprojecttest_2.10-0.1.0.jar
  */

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UdidCount {

  case class SBLog(date: String, version: String, udid: String)

  case class StartLog(udid: String, time: Long)

  case class ELog(udid: String, protol_type: String, start_id: Long, time: Long)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UdidCount").setMaster("local[*]")
    //    val outputPath: File = new File("/Users/zhaogj/bangcle/southbase/output")
    //    if (outputPath.exists()) {
    //      outputPath.delete()
    //    }
    val spark = SparkSession
      .builder().config(conf)
      .getOrCreate()

    import spark.implicits._
    //    val sblogFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/sblog")
    //    val sblogDF = sblogFile
    //      .map(_.split("\t"))
    //      .filter(_.size == 3)
    //      .map(attributes => SBLog(attributes(0).trim, attributes(1).trim, attributes(2).trim))
    //      .toDF()
    //    sblogDF.createOrReplaceTempView("sblog")
    //    val startlogFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/startlog")
    //    val startlogDF = startlogFile
    //      .map(_.split(","))
    //      .filter(_.size == 2)
    //      .map(x => StartLog(x(0).trim, x(1).trim.toLong))
    //      .toDF()
    //    startlogDF.createOrReplaceTempView("startlog")

    //    val teenagersDF = spark.sql("SELECT DISTINCT date, version, udid FROM sblog WHERE udid != '\\N'")
    //    teenagersDF.map(teenager => teenager.getAs[String]("version") + " " + teenager.getAs[String]("date")).show()
    //    val startlogsqlDF = spark.sql("SELECT DISTINCT udid FROM startlog WHERE udid != '\\N'")
    //    startlogsqlDF.map(teenager => teenager.getAs[String]("udid")).show()
    //    val joinDF = spark.sql("SELECT sblog.version, startlog.udid FROM sblog, startlog WHERE sblog.udid = startlog.udid")
    //    joinDF.map(teenager => teenager.getAs[String]("udid") + " " + teenager.getAs[String]("version")).show()
    //    val sbCountDF = spark.sql("SELECT COUNT(udid) FROM sblog")
    //    sbCountDF.map(x => "sblog:" + x(0)).show()
    //
    //    val sbCountDisDF = spark.sql("SELECT COUNT(DISTINCT udid) FROM sblog")
    //    sbCountDisDF.map(x => "sbdisudid:" + x(0)).show()
    //
    //    val startCountDF = spark.sql("SELECT COUNT(udid) FROM startlog")
    //    startCountDF.map(x => "start:" + x(0)).show()
    //
    //    val startDisCountDF = spark.sql("SELECT COUNT(DISTINCT udid) FROM startlog")
    //    startDisCountDF.map(x => "startdisudid:" + x(0)).show()
    //
    //    val joinDF = spark.sql("SELECT COUNT(startlog.udid) FROM sblog, startlog WHERE sblog.udid = startlog.udid")
    //    joinDF.map(x => "joinudid:" + x(0)).show()
    //
    //    val joinDisDF = spark.sql("SELECT COUNT(DISTINCT startlog.udid) FROM sblog, startlog WHERE sblog.udid = startlog.udid")
    //    joinDisDF.map(x => "joindisudid:" + x(0)).show()
    //    val joinDF = spark.sql("SELECT COUNT(DISTINCT sblog.version) FROM sblog, startlog WHERE sblog.udid = startlog.udid")
    //    joinDF.map(x => "count:" + x(0)).show()
    //计算交集中版本占比
    //    val joinDF = spark.sql("SELECT sblog.version, COUNT(sblog.version) FROM sblog, startlog WHERE sblog.udid = startlog.udid GROUP BY sblog.version ORDER BY count(version) DESC")
    //    joinDF.printSchema()
    //joinDF.map(x => x(0) + ":" + x(1)).show()
    //    joinDF.select($"version", $"COUNT(version)").show()
    //    val joinDF = spark.sql("SELECT version, COUNT(version) FROM sblog GROUP BY version ORDER BY count(version) DESC")
    //    joinDF.map(x => x(0) + ":" + x(1)).show()
    //    joinDF.select($"version", $"COUNT(version)").show()
    //计算设备版本分布
    //    val joinDF = spark.sql("SELECT * FROM sblog")
    //    //    joinDF.printSchema()
    //    //    joinDF.groupBy("version").count().orderBy($"count".desc).show()
    //    joinDF.groupBy("version").count().orderBy($"count".desc).write.json("/Users/zhaogj/bangcle/southbase/output")
    //    计费数据总量
    //    val joinDF = spark.sql("SELECT count(*) FROM sblog")
    //    joinDF.map(x => "" + x(0)).show()

    //        val joinDF = spark.sql("SELECT * FROM sblog")
    //        joinDF.groupBy("udid").count().orderBy($"count".desc).show()
    //    joinDF.groupBy("udid").count().orderBy($"count".desc).write.json("/Users/zhaogj/bangcle/southbase/output")
    //过滤掉不合法的udid
    //    val joinDF = spark.sql("SELECT * FROM sblog WHERE udid not in ('\\\\N', 'null')")
    //    joinDF.groupBy("udid").count().orderBy($"count".desc).show()
    //    //    joinDF.groupBy("udid").count().orderBy($"count".desc).write.json("/Users/zhaogj/bangcle/southbase/output")

    //以下为计算逻辑
    //计费数据udid不合法总量
    //    val joinDF = spark.sql("SELECT count(*) FROM sblog WHERE udid in ('\\\\N', 'null')")
    //    joinDF.map(x => "" + x(0)).show()
    //计费数据udid去重总量
    //    val joinDF = spark.sql("SELECT count(distinct udid) FROM sblog WHERE udid not in ('\\\\N', 'null')")
    //    joinDF.map(x => "" + x(0)).show()

    //启动数据条数
    //    val joinDF = spark.sql("SELECT count(*) FROM startlog")
    //    joinDF.map(x => "" + x(0)).show()

    //启动数据设备去重
    //    val joinDF = spark.sql("SELECT count(distinct udid) FROM startlog")
    //    joinDF.map(x => "" + x(0)).show()
    //    joinDF.groupBy("udid").count().orderBy($"count".desc).show()

    //匹配数量，去重
    //    val joinDF = spark.sql("SELECT COUNT(distinct startlog.udid) FROM sblog, startlog WHERE sblog.udid not in ('\\\\N', 'null') AND sblog.udid = startlog.udid")
    //    joinDF.map(x => "" + x(0)).show()

    //start
    val eStartFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/start/")
    val eStartDF = eStartFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eStartDF.createOrReplaceTempView("e_start_log")

    //attframe
    val eAttframeFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/attframe/")
    val eAttframeDF = eAttframeFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eAttframeDF.createOrReplaceTempView("e_attframe_log")

    //badprefs
    val eBadprefsFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/badprefs/")
    val eBadprefsDF = eBadprefsFile
      .map(_.split("\t"))
      .filter(_.size >= 11)
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eBadprefsDF.createOrReplaceTempView("e_badprefs_log")

    //cheatdns
    val eCheatdnsFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/cheatdns/")
    val eCheatdnsDF = eCheatdnsFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eCheatdnsDF.createOrReplaceTempView("e_cheatdns_log")

    //chook
    val eChookFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/chook/")
    val eChookDF = eChookFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eChookDF.createOrReplaceTempView("e_chook_log")

    //cpuflux
    val eCpufluxFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/cpuflux/")
    val eCpufluxDF = eCpufluxFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eCpufluxDF.createOrReplaceTempView("e_cpuflux_log")

    //emulator
    val eEmulatorFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/emulator/")
    val eEmulatorDF = eEmulatorFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eEmulatorDF.createOrReplaceTempView("e_emulator_log")

    //inject
    val eInjectFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/inject/")
    val eInjectDF = eInjectFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eInjectDF.createOrReplaceTempView("e_inject_log")

    //isroot
    val eIsrootFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/isroot/")
    val eIsrootDF = eIsrootFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eIsrootDF.createOrReplaceTempView("e_isroot_log")

    //location
    val eLocationFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/location/")
    val eLocationDF = eLocationFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eLocationDF.createOrReplaceTempView("e_location_log")

    //mockdev
    val eMockdevFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/mockdev/")
    val eMockdevDF = eMockdevFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eMockdevDF.createOrReplaceTempView("e_mockdev_log")

    //syssetting
    val eSyssettingFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/syssetting/")
    val eSyssettingDF = eSyssettingFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eSyssettingDF.createOrReplaceTempView("e_syssetting_log")

    //xemu
    val eXemuFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/eversk3.x_20190725/xemu/")
    val eXemuDF = eXemuFile
      .map(_.split("\t"))
      .map(x => ELog(x(4).trim, x(9).trim, x(2).trim.toLong, x(10).trim.toLong))
      .toDF()
    eXemuDF.createOrReplaceTempView("e_xemu_log")

    //sql
    var sqlDF = spark.sql("SELECT * FROM e_start_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_attframe_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_chook_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_badprefs_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_cheatdns_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_cpuflux_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_emulator_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_inject_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_isroot_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_location_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_mockdev_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_syssetting_log")
    //    sqlDF.show()
    //    sqlDF = spark.sql("SELECT * FROM e_xemu_log")
    //    sqlDF.show()

    //按照协议类型统计日志条数
    sqlDF = spark.sql("SELECT protol_type, count(*) from " +
      "(SELECT * FROM e_start_log " +
      "UNION ALL SELECT * FROM e_attframe_log " +
      "UNION ALL SELECT * FROM e_chook_log " +
      "UNION ALL SELECT * FROM e_badprefs_log " +
      "UNION ALL SELECT * FROM e_cheatdns_log " +
      "UNION ALL SELECT * FROM e_cpuflux_log " +
      "UNION ALL SELECT * FROM e_emulator_log " +
      "UNION ALL SELECT * FROM e_inject_log " +
      "UNION ALL SELECT * FROM e_isroot_log " +
      "UNION ALL SELECT * FROM e_location_log " +
      "UNION ALL SELECT * FROM e_mockdev_log " +
      "UNION ALL SELECT * FROM e_syssetting_log " +
      "UNION ALL SELECT * FROM e_xemu_log) GROUP BY protol_type")
    //    sqlDF.show()
    //启动表中udid,start_id去重数
    sqlDF = spark.sql("SELECT count(distinct udid, start_id) from e_start_log")
    //    sqlDF.show()
    //威胁表中udid,start_id去重数
    sqlDF = spark.sql("SELECT count(distinct t_log.udid, t_log.start_id) from " +
      "(SELECT * FROM e_attframe_log " +
      "UNION ALL SELECT * FROM e_chook_log " +
      "UNION ALL SELECT * FROM e_badprefs_log " +
      "UNION ALL SELECT * FROM e_cheatdns_log " +
      "UNION ALL SELECT * FROM e_cpuflux_log " +
      "UNION ALL SELECT * FROM e_emulator_log " +
      "UNION ALL SELECT * FROM e_inject_log " +
      "UNION ALL SELECT * FROM e_isroot_log " +
      "UNION ALL SELECT * FROM e_location_log " +
      "UNION ALL SELECT * FROM e_mockdev_log " +
      "UNION ALL SELECT * FROM e_syssetting_log " +
      "UNION ALL SELECT * FROM e_xemu_log) AS t_log")
    //sqlDF.show()
    //交集
    sqlDF = spark.sql("SELECT count(distinct t_log.udid, t_log.start_id) from " +
      "(SELECT * FROM e_attframe_log " +
      "UNION ALL SELECT * FROM e_chook_log " +
      "UNION ALL SELECT * FROM e_badprefs_log " +
      "UNION ALL SELECT * FROM e_cheatdns_log " +
      "UNION ALL SELECT * FROM e_cpuflux_log " +
      "UNION ALL SELECT * FROM e_emulator_log " +
      "UNION ALL SELECT * FROM e_inject_log " +
      "UNION ALL SELECT * FROM e_isroot_log " +
      "UNION ALL SELECT * FROM e_location_log " +
      "UNION ALL SELECT * FROM e_mockdev_log " +
      "UNION ALL SELECT * FROM e_syssetting_log " +
      "UNION ALL SELECT * FROM e_xemu_log) AS t_log , e_start_log WHERE t_log.udid = e_start_log.udid AND t_log.start_id = e_start_log.start_id")
    //    sqlDF.show()
    //没有启动的设备数量
    sqlDF = spark.sql("SELECT count(distinct t_log.udid, t_log.start_id) from " +
      "(SELECT * FROM e_attframe_log " +
      "UNION ALL SELECT * FROM e_chook_log " +
      "UNION ALL SELECT * FROM e_badprefs_log " +
      "UNION ALL SELECT * FROM e_cheatdns_log " +
      "UNION ALL SELECT * FROM e_cpuflux_log " +
      "UNION ALL SELECT * FROM e_emulator_log " +
      "UNION ALL SELECT * FROM e_inject_log " +
      "UNION ALL SELECT * FROM e_isroot_log " +
      "UNION ALL SELECT * FROM e_location_log " +
      "UNION ALL SELECT * FROM e_mockdev_log " +
      "UNION ALL SELECT * FROM e_syssetting_log " +
      "UNION ALL SELECT * FROM e_xemu_log) AS t_log WHERE t_log.udid in(select udid from e_start_log)")
    sqlDF.show()
    //没有威胁的设备数量
    sqlDF = spark.sql("select count(distinct udid, start_id) from e_start_log where udid + start_id not in (SELECT t_log.udid + t_log.start_id from " +
      "(SELECT * FROM e_attframe_log " +
      "UNION ALL SELECT * FROM e_chook_log " +
      "UNION ALL SELECT * FROM e_badprefs_log " +
      "UNION ALL SELECT * FROM e_cheatdns_log " +
      "UNION ALL SELECT * FROM e_cpuflux_log " +
      "UNION ALL SELECT * FROM e_emulator_log " +
      "UNION ALL SELECT * FROM e_inject_log " +
      "UNION ALL SELECT * FROM e_isroot_log " +
      "UNION ALL SELECT * FROM e_location_log " +
      "UNION ALL SELECT * FROM e_mockdev_log " +
      "UNION ALL SELECT * FROM e_syssetting_log " +
      "UNION ALL SELECT * FROM e_xemu_log) AS t_log)")
    //    sqlDF.show()

    //
    //    sqlDF = spark.sql("SELECT count(*) FROM e_start_log")
    //    sqlDF.map(x => "logcount:" + x(0)).show()
    //    sqlDF = spark.sql("SELECT count(distinct udid) FROM e_start_log")
    //    sqlDF.map(x => "udidcount:" + x(0)).show()


    spark.stop()
  }
}
