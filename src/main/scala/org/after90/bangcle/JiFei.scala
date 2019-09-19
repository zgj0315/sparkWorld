package org.after90.bangcle

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object JiFei {

  case class JFJQ(date: String, time: String, phone: String, app_id: String, imsi: String, imei: String, sdk_ver: String, udid: String)

  def main(args: Array[String]): Unit = {
    //    jq()
    //    phone()
    dg()
  }

  def jq(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/jifei/jianquan/20190809鉴权数据_手机号.txt")
    val jqDF = jqFile
      .map(_.split("\t"))
      .filter(_.size == 8)
      .map(x => JFJQ(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim, x(7).trim))
      .toDF()
    jqDF.createOrReplaceTempView("jq")
    val sqlDF = spark.sql("select * from jq where phone = 'D41D8CD98F00B204E9800998ECF8427E'")
    sqlDF.groupBy("udid").count().sort($"count".desc).show(1000)
    //    sqlDF.groupBy("phone").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/output_phone")
    //    sqlDF.show()

    spark.stop()
  }

  case class Phone(phone: String, count: Integer)

  def phone(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/jf/output_phone/part*")
    val jqDF = jqFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => Phone(x(0).trim, Integer.parseInt(x(1).trim)))
      .toDF()
    jqDF.createOrReplaceTempView("jq")
    val sqlDF = spark.sql("select sum(count) from jq where count > 5")
    sqlDF.show()

    spark.stop()
  }

  case class JFDG(statis_data: String, req_time: String, session_id: String, mobile_no: String,
                  user_type: String, app_id: String, fee_id: String, app_type: String, fee: Integer,
                  imsi: String, imei: String, acc_net_type: String, ip: String, pay_type: String,
                  port_ver: String, sdk_ver: String, mk_chan_id: String, fee_barck_id: String,
                  res_time: String, return_code: String, order_id: String, deal_dur: String,
                  conn_net_type: String, prog_type: String, app_os_id: String, pid: String,
                  miniversion: String, udid: String, businesscode: String, msisdntype: String,
                  traceid: String, totalprice: String, subsnumb: String, authtype: String, resourceid: String)

  def dg(): Unit = {
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
    val sqlDF = spark.sql("select imsi, sum(fee) as fee_sum from dg group by imsi order by fee_sum desc")
    //    sqlDF.groupBy("imsi").count().sort($"count".desc).show(100)
    //    sqlDF.groupBy("phone").count().sort($"count".desc).repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jf/output_phone")
    sqlDF.show()

    spark.stop()
  }
}
