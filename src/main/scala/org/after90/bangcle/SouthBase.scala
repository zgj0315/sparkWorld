package org.after90.bangcle

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SouthBase {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /**
      * create view jq
      */
    jq(spark)
    //    val sqlDF = spark.sql("select * from jq where phone = 'D41D8CD98F00B204E9800998ECF8427E'")
    //    sqlDF.groupBy("udid").count().sort($"count".desc).show(1000)
    //    sqlDF.groupBy("phone").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/output_phone")
    //    sqlDF.show()
    /**
      * create view phone
      */
    phone(spark)
    //    val sqlDF = spark.sql("select sum(count) from jq where count > 5")
    //    sqlDF.show()
    /**
      * create view dg
      */
    dg(spark)
    //    val sqlDF = spark.sql("select imsi, sum(fee) as fee_sum from dg group by imsi order by fee_sum desc")
    //    sqlDF.groupBy("imsi").count().sort($"count".desc).show(100)
    //    sqlDF.groupBy("phone").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/output_phone")
    //    sqlDF.show()
    /**
      * create view start
      */
    start(spark)
    //    val sqlDF = spark.sql("select * from start")
    //    sqlDF.show()
    /**
      * create view dev_v3, start_v3, dev_v4 and start_v4
      */
    v3(spark)
    v4(spark)

    /**
      * 鉴权的手机号是否都有启动数据
      */
    // 启动手机号个数
    //    val sqlDF = spark.sql("select count(distinct (mobileno)) from start")
    //    sqlDF.show()
    //    2092018
    // 鉴权手机号个数
    //    val sqlDF = spark.sql("select count(distinct (mobile_no)) from jq")
    //    sqlDF.show()
    //    82210

    // 有启动数据的鉴权手机号个数
    //    val sqlDF = spark.sql("select count(distinct (a.mobile_no)) from jq as a, start as b where a.mobile_no = b.mobileno")
    //    sqlDF.show()
    //    79169 79169/82210=96.3% 没有启动直接鉴权，是否合法？
    /**
      * 订购的手机号是否都有启动和鉴权
      */
    // 订购手机号个数
    //    val sqlDF = spark.sql("select count(distinct mobile_no) from dg")
    //    sqlDF.show()
    //    42835
    // 有启动数据的订购手机号个数
    //    val sqlDF = spark.sql("select count(distinct a.mobile_no) from dg as a, start as b where a.mobile_no = b.mobileno")
    //    sqlDF.show()
    //    41248 41248/42835 = 96.3% 和鉴权很像啊
    // 有鉴权数据的订购手机号个数
    //    val sqlDF = spark.sql("select count(distinct a.mobile_no) from dg as a, jq as b where a.mobile_no = b.mobile_no")
    //    sqlDF.show()
    //    42832 42832/42835 = 99.99% 完美匹配

    /**
      * 订购udid和威胁感知udid匹配
      */
    // v3
    //    val sqlDF = spark.sql("select count(distinct udid) from dev_v3")
    //    sqlDF.show()
    //    883958
    //    val sqlDF = spark.sql("select count(distinct udid) from start_v3")
    //    sqlDF.show()
    //    699929
    //    val sqlDF = spark.sql("select count(distinct a.udid) from start_v3 as a, dev_v3 as b where a.udid = b.udid")
    //    sqlDF.show()
    //    133839 少部分udid在设备表中，奇怪了。要对比就要用全集了

    // v4
    //    val sqlDF = spark.sql("select count(distinct udid) from dev_v4")
    //    sqlDF.show()
    //    1848917
    //    val sqlDF = spark.sql("select count(distinct udid) from start_v4")
    //    sqlDF.show()
    //    1847206

    //    val sqlDF = spark.sql("select count(distinct a.udid) from start_v4 as a, dev_v4 as b where a.udid = b.udid")
    //    sqlDF.show()
    //    1844312 完美匹配

    // 订购udid个数
    //    val sqlDF = spark.sql("select count(distinct udid) from dg")
    //    sqlDF.show()
    //    49769
    // 订购udid在v3中的个数
    //    val sqlDF = spark.sql("select count(distinct a.udid) from dg as a, " +
    //      "(select udid from dev_v3 union select udid from start_v3) as b " +
    //      "where a.udid = b.udid")
    //    sqlDF.show()
    //    719 匹配率少的想哭
    // 订购udid在v4中的个数
    //    val sqlDF = spark.sql("select count(distinct a.udid) from dg as a, " +
    //      "dev_v4 as b " +
    //      "where a.udid = b.udid")
    //    sqlDF.show()
    //    2621 比v3好一些，然并卵
    // 订购udid在v3+v4中的个数
    //    val sqlDF = spark.sql("select count(distinct a.udid) from dg as a, " +
    //      "(select udid from dev_v3 union select udid from dev_v4) as b " +
    //      "where a.udid = b.udid")
    //    sqlDF.show()
    //    2918 2918/49769=5.86% 这个匹配率，跪着吧，别分析了，把精力拉回到计费数据的分析上

    /**
      * 分析计费数据
      */
    // 各个字段维度，重复情况分析
    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "and udid in (select a.udid from dg as a, (select udid from dev_v3 union select udid from dev_v4) as b " +
    //      "where a.udid = b.udid)")
    //    sqlDF.groupBy("udid", "mobile_no").count().sort($"count".desc).show()

    //    val sqlDF = spark.sql("select count(distinct udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF'")

    //    val sqlDF = spark.sql("select * from dg where udid = '24c8917a-e140-35b6-a24b-01adbda5e1bf'")
    //    sqlDF.show()

    //    sqlDF.groupBy("udid").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/udid")

    //    sqlDF.groupBy("mobile_no").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/mobile_no")

    //    sqlDF.groupBy("udid").count().sort($"count".desc).show()

    // 剔除68BAC75ED032E0A86BC3EAE1B5C996CF号码后的匹配率
    // 订购udid个数
    //    val sqlDF = spark.sql("select count(distinct udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF'")
    //    sqlDF.show()
    //    49757
    // 订购udid在v3+v4中的个数
    //    val sqlDF = spark.sql("select count(distinct a.udid) from dg as a, " +
    //      "(select udid from dev_v3 union select udid from dev_v4) as b " +
    //      "where a.mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and a.udid = b.udid")
    //    sqlDF.show()

    /**
      * 20190920
      */

    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF'")
    //    sqlDF.groupBy("imei").count().groupBy("count").count().sort($"count".desc).show(100)
    //              .repartition(1).write.format("csv").save("/Users/zhaogj/tmp/output")
    //        sqlDF.show()
    // v3 udid个数
    //    val sqlDF = spark.sql("select count(distinct udid) from (select udid from dev_v3 union select udid from start_v3)")
    // v4 udid个数
    //    val sqlDF = spark.sql("select count(distinct udid) from (select udid from dev_v4 union select udid from start_v4)")
    // 订购数据条数
    //    val sqlDF = spark.sql("select count(*) from dg as a, (select udid from dev_v3 union select udid from start_v3) as b where a.mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and a.udid = b.udid")
    //    1054
    //    val sqlDF = spark.sql("select count(*) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from dev_v3 union select udid from start_v3)")
    //    1054

    // 订购udid个数
    //    val sqlDF = spark.sql("select count(distinct a.udid) from dg as a, (select udid from dev_v3 union select udid from start_v3) as b where a.mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and a.udid = b.udid")
    //    707
    //    val sqlDF = spark.sql("select count(distinct udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from dev_v3 union select udid from start_v3)")
    //    707
    // v4匹配
    //    val sqlDF = spark.sql("select count(udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from dev_v4 union select udid from start_v4)")
    //    3606
    //    val sqlDF = spark.sql("select count(distinct udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from dev_v4 union select udid from start_v4)")
    //    2622
    //    val sqlDF = spark.sql("select distinct udid from (select udid from dev_v3 union select udid from start_v3 union select udid from dev_v4 union select udid from start_v4)")
    //    sqlDF.repartition(1).write.parquet("/Users/zhaogj/bangcle/southbase/output/everisk/udid.parquet")
    spark.read.parquet("/Users/zhaogj/bangcle/southbase/output/everisk/udid.parquet").createOrReplaceTempView("everisk_udid")
    // v3+v4
    //    val sqlDF = spark.sql("select distinct udid from dg where " +
    //      "mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and " +
    //      "udid in (select udid from everisk_udid)")
    // 匹配上的udid集合
    //    sqlDF.repartition(1).write.parquet("/Users/zhaogj/bangcle/southbase/output/join/udid.parquet")
    spark.read.parquet("/Users/zhaogj/bangcle/southbase/output/join/udid.parquet").createOrReplaceTempView("join_udid")

    // 匹配上的udid特征
    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from join_udid)")
    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid not in (select udid from join_udid)")
    //    sqlDF.groupBy("ip").count().sort($"count".desc).show()

    //    sqlDF.groupBy("sdk_ver").count().groupBy("count").count().sort($"count".desc).show()
    //    val sqlDF = spark.sql("select mobile_no, count(distinct (udid)) as count from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and " +
    //      "udid not in (select udid from join_udid) group by mobile_no order by count desc")
    //    sqlDF.groupBy("count").count().sort($"count").show()
    //    sqlDF.show()
    // imei匹配情况分析
    //    val sqlDF = spark.sql("select count(imei) from dg")
    //    val sqlDF = spark.sql("select count(imei) from dg where imei in (select imei from dev_v4 union select imei from dev_v3)")
    //    sqlDF.show()
    //    val sqlDF = spark.sql("select * from dg where mobile_no = '68BAC75ED032E0A86BC3EAE1B5C996CF'")
    //    sqlDF.groupBy("udid").count().sort($"count".desc).show(20, false)

    //    val sqlDF = spark.sql("select distinct udid from (select udid from dev_v3 union select udid from start_v3) where udid in (select udid from dg where mobile_no = '68BAC75ED032E0A86BC3EAE1B5C996CF')")
    //    sqlDF.show()
    //    val sqlDF = spark.sql("select * from dg")
    //    sqlDF.groupBy("mobile_no").count().sort($"count".desc).show(10, false)
    //    val sqlDF = spark.sql("select count(distinct udid) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and udid in (select udid from everisk_udid)")
    //    sqlDF.show()
    //    sqlDF.groupBy("udid").count().sort($"count".desc).show(20, false)
    //    val sqlDF = spark.sql("select mobile_no, count(distinct udid) as count from dg where udid in (select udid from join_udid) and mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "group by mobile_no order by count desc")
    //    sqlDF.groupBy("mobile_no").count().sort($"count".desc).show(20, false)
    //    spark.sql("select mobile_no, count(distinct udid) as count from dg where udid not in (select udid from join_udid) and mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "group by mobile_no order by count desc").createOrReplaceTempView("mobile_no")
    //    val sqlDF = spark.sql("select sum(fee) from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and mobile_no in (select mobile_no from mobile_no where count >= 2)")
    //    sqlDF.show(20, false)

    //    val sqlDF = spark.sql("select distinct imei from (select imei from dev_v3 union select imei from dev_v4)")
    //    sqlDF.repartition(1).write.parquet("/Users/zhaogj/bangcle/southbase/output/everisk/imei.parquet")
    spark.read.parquet("/Users/zhaogj/bangcle/southbase/output/everisk/imei.parquet").createOrReplaceTempView("everisk_imei")
    //    val sqlDF = spark.sql("select distinct imei from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and imei in (select imei from everisk_imei)")
    //    sqlDF.repartition(1).write.parquet("/Users/zhaogj/bangcle/southbase/output/join/imei.parquet")
    spark.read.parquet("/Users/zhaogj/bangcle/southbase/output/join/imei.parquet").createOrReplaceTempView("join_imei")

    //    val sqlDF = spark.sql("select mobile_no, count(distinct imei) as count from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "and imei in (select imei from join_imei) " +
    //      "group by mobile_no order by count desc")
    //    sqlDF.show()

    //    spark.sql("select mobile_no, count(distinct imei) as count from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "and imei not in (select imei from join_imei) " +
    //      "group by mobile_no order by count desc").createOrReplaceTempView("mobile_no")
    //    val sqlDF = spark.sql("select sum(fee) from dg where  mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' and mobile_no in (select mobile_no from mobile_no where count >=2)")
    //    sqlDF.show()
    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF' " +
    //      "and imei not in (select imei from join_imei)")
    //    sqlDF.groupBy("imei").count().sort($"count".desc).show(20, false)
    //    val sqlDF = spark.sql("select * from dg where mobile_no <> '68BAC75ED032E0A86BC3EAE1B5C996CF'")
    //    sqlDF.groupBy("imei").count().sort($"count".desc).show(20, false)

    /**
      * sdk版本分析
      */
    sdkVersionStart(spark)
    sdkVersionCmcc(spark)
    //    val sqlDF = spark.sql("select count(distinct udid) from cmcc")
    //    sqlDF.groupBy("udid").count().sort($"count").show(100, false)
    //    sqlDF.show(20, false)

    // 埋点数据中设备个数:1606848
    //    val sqlDF = spark.sql("select count(distinct udid) from start_0815")
    //    sqlDF.show()
    // 威胁感知中设备个数：658681
    //    val sqlDF = spark.sql("select count(distinct udid) from start_0815 where udid in (select udid from cmcc)")
    //    val sqlDF = spark.sql("select count(distinct udid) from cmcc where udid in (select udid from start_0815)")
    //    sqlDF.show()
    // 交集的设备个数：601138
    //    val sqlDF = spark.sql("select a.udid, a.sdk_ver from cmcc as a, start_0815 as b where a.udid = b.udid")
    //    sqlDF.groupBy("sdk_ver").count().sort($"count").show(100, false)
    //    sqlDF.show()
    val sqlDF = spark.sql("select sdk_ver, count(distinct udid) as count from cmcc where udid in (select udid from start_0815) group by sdk_ver order by count desc")
    sqlDF.show()
    spark.stop()
  }

  case class JFJQ(statis_date: String, req_time: String, seq_id: String, session_id: String,
                  mobile_no: String, user_id: String, user_type: String, app_id: String, fee_id: String,
                  app_type: String, fee: String, imsi: String, imei: String, trem_type: String,
                  os: String, acc_net_type: String, ip: String, port_ver: String, sdk_ver: String,
                  mk_chan_id: String, fee_barck_id: String, res_time: String, return_code: String,
                  order_id: String, deal_dur: String, conn_net_type: String, prog_type: String,
                  app_os_id: String, pid: String, miniversion: String, udid: String, businesscode: String,
                  vmmode: String, authtype: String, traceid: String, authseq: String, resourceid: String)

  def jq(spark: SparkSession): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/jifei/jianquan/20190809_鉴权数据_全字段1")
    val jqDF = jqFile
      .map(_.split("\t"))
      .filter(_.size == 37)
      .map(x => JFJQ(x(1).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim,
        x(7).trim, x(8).trim, x(9).trim, x(10).trim, x(11).trim, x(12).trim, x(13).trim, x(14).trim,
        x(15).trim, x(16).trim, x(17).trim, x(18).trim, x(19).trim, x(20).trim, x(21).trim, x(22).trim,
        x(23).trim, x(24).trim, x(25).trim, x(26).trim, x(27).trim, x(28).trim, x(29).trim, x(30).trim,
        x(31).trim, x(32).trim, x(33).trim, x(34).trim, x(35).trim, x(36).trim))
      .toDF()
    jqDF.createOrReplaceTempView("jq")
  }

  case class Phone(mobile_no: String, count: Integer)

  def phone(spark: SparkSession): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/jf/output_phone/part*")
    val jqDF = jqFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => Phone(x(0).trim, Integer.parseInt(x(1).trim)))
      .toDF()
    //    jqDF.createOrReplaceTempView("jq")
  }

  case class JFDG(statis_data: String, req_time: String, session_id: String, mobile_no: String,
                  user_type: String, app_id: String, fee_id: String, app_type: String, fee: Integer,
                  imsi: String, imei: String, acc_net_type: String, ip: String, pay_type: String,
                  port_ver: String, sdk_ver: String, mk_chan_id: String, fee_barck_id: String,
                  res_time: String, return_code: String, order_id: String, deal_dur: String,
                  conn_net_type: String, prog_type: String, app_os_id: String, pid: String,
                  miniversion: String, udid: String, businesscode: String, msisdntype: String,
                  traceid: String, totalprice: String, subsnumb: String, authtype: String, resourceid: String)

  def dg(spark: SparkSession): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val file = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/jifei/dinggou/20190809订购数据_全字段.txt")
    val df = file
      .map(_.split("\t"))
      .filter(_.size == 35)
      .map(x => JFDG(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim,
        x(7).trim, Integer.parseInt(x(8).trim), x(9).trim, x(10).trim, x(11).trim, x(12).trim, x(13).trim, x(14).trim,
        x(15).trim, x(16).trim, x(17).trim, x(18).trim, x(19).trim, x(20).trim, x(21).trim, x(22).trim,
        x(23).trim, x(24).trim, x(25).trim, x(26).trim, x(27).trim, x(28).trim, x(29).trim, x(30).trim
        , x(31).trim, x(32).trim, x(33).trim, x(34).trim))
      .toDF()
    df.createOrReplaceTempView("dg")
  }

  case class Start(statis_data: String, app_id: String, app_user_id: String, sdk_ver: String,
                   pkg_ver: String, deviceid: String, channel_id: String, mobileno: String,
                   os: String, term: String, netwk: String, cust_cluster: String, to_brand: String,
                   imei: String, imsi: String, screen: String, prov: String, upflow: String,
                   downflow: String, session_dur: String, stat_cnt: String, network_type: String,
                   first_start_time: String, last_start_time: String, p_mon_cd: String,
                   p_data_cd: String)

  private def start(spark: SparkSession): Unit = {
    import spark.implicits._
    val file = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/jifei/start/20190809启动数据_全字段.txt")
    val df = file
      .map(_.split("\t"))
      .filter(_.size == 26)
      .map(x => Start(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim,
        x(7).trim, x(8).trim, x(9).trim, x(10).trim, x(11).trim, x(12).trim, x(13).trim, x(14).trim,
        x(15).trim, x(16).trim, x(17).trim, x(18).trim, x(19).trim, x(20).trim, x(21).trim, x(22).trim,
        x(23).trim, x(24).trim, x(25).trim))
      .toDF()
    df.createOrReplaceTempView("start")
  }

  case class EDevV3(udid: String, //4
                    imei: String, //18
                    manufacturer: String, //19
                    manufacturer_prop: String, //20
                    model: String, //21
                    model_prop: String, //22
                    imsi: String, //25
                    sdk_version: String, //27
                    os_version: String, //28
                    os_name: String //29
                   )

  case class EStartV3(udid: String, //4
                      market: String, //12
                      pname: String, //19
                      manu_manufacturer: String, //23
                      manu_os_version: String, //24
                      apk_app_name: String, //25
                      apk_ver_name: String //26
                     )

  private def v3(spark: SparkSession): Unit = {
    import spark.implicits._

    val devFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v3.0/dev/")
    val devDF = devFile
      .map(_.split("\t"))
      .map(x => EDevV3(x(4).trim, x(18).trim, x(19).trim, x(20).trim, x(21).trim, x(22).trim, x(25).trim, x(27).trim, x(28).trim, x(29).trim))
      .toDF()
    devDF.createOrReplaceTempView("dev_v3")
    val startFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v3.0/start/20190809/")
    val startDF = startFile
      .map(_.split(","))
      .map(x => EStartV3(x(4).trim, x(12).trim, x(19).trim, x(23).trim, x(24).trim, x(25).trim, x(26).trim))
      .toDF()
    startDF.createOrReplaceTempView("start_v3")
  }

  case class EDevV4(udid: String,
                    imei: String,
                    manufacturer: String,
                    manufacturer_prop: String,
                    model: String,
                    model_prop: String,
                    imsi: String,
                    sdk_version: String,
                    os_version: String,
                    os_name: String
                   )

  case class EStartV4(udid: String,
                      market: String,
                      pname: String,
                      app_name: String,
                      app_version: String
                     )

  private def v4(spark: SparkSession): Unit = {
    import spark.implicits._

    val devFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v4.3/dev/*.txt")
    val devDF = devFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .filter(_.containsKey("imei"))
      .map(x => EDevV4(x.getString("udid"), x.getString("imei"), x.getString("manufacturer"),
        x.getString("manufacturer_prop"), x.getString("model"), x.getString("model_prop"),
        x.getString("imsi"), x.getString("sdk_version"), x.getString("os_version"),
        x.getString("os_name")))
      .flatMap(x => {
        val imeis = x.imei.split(",")
        val arr = new Array[EDevV4](imeis.size)
        for (i <- 0 until imeis.size) {
          arr(i) = EDevV4(x.udid,
            imeis(i).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""),
            x.manufacturer, x.manufacturer_prop, x.model, x.model_prop, x.imsi, x.sdk_version, x.os_version, x.os_name)
        }
        arr
      })
      .flatMap(x => {
        val imsis = x.imsi.split(",")
        val arr = new Array[EDevV4](imsis.size)
        for (i <- 0 until imsis.size) {
          arr(i) = EDevV4(x.udid, x.imei, x.manufacturer, x.manufacturer_prop, x.model, x.model_prop,
            imsis(i).replaceAll("\"", "").replaceAll("\\[", "").replaceAll("\\]", ""),
            x.sdk_version, x.os_version, x.os_name)
        }
        arr
      })
      .toDF()
    devDF.createOrReplaceTempView("dev_v4")

    val startFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v4.3/start/*.txt")
    val startDF = startFile
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .map(x => {
        val start = x.getJSONObject("start")
        EStartV4(x.getString("udid"), x.getString("market"), start.getString("pname"),
          start.getString("app_name"), start.getString("app_version"))
      })
      .toDF()
    startDF.createOrReplaceTempView("start_v4")
  }

  private def sdkVersionStart(spark: SparkSession): Unit = {
    import spark.implicits._
    val file = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v4.3/sdk_ver/2019_0815_start.txt")
    val df = file
      .map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .map(x => {
        val start = x.getJSONObject("start")
        EStartV4(x.getString("udid"), x.getString("market"), start.getString("pname"),
          start.getString("app_name"), start.getString("app_version"))
      })
      .toDF()
    df.createOrReplaceTempView("start_0815")
  }

  case class CMCC(udid: String, sdk_ver: String)

  private def sdkVersionCmcc(spark: SparkSession): Unit = {
    import spark.implicits._
    val file = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/v4.3/sdk_ver/*cmccdata/*")
    val df = file
      .map(_.split(","))
      .filter(_.size >= 24)
      .map(x => {
        if ("cmccdata2".equals(x(9)) || "cmccdata8".equals(x(9))) {
          CMCC(x(4), x(23))
        } else if ("cmccdata11".equals(x(9)) || "cmccdata21".equals(x(9)) || "cmccdata23".equals(x(9))) {
          CMCC(x(4), x(21))
        } else if ("cmccdata10".equals(x(9)) || "cmccdata22".equals(x(9))) {
          CMCC(x(4), x(22))
        } else if ("cmccdata3".equals(x(9)) || "cmccdata9".equals(x(9))) {
          CMCC(x(4), x(24))
        } else {
          CMCC(x(4), "null")
        }
      })
      .toDF()
    df.createOrReplaceTempView("cmcc")
  }
}
