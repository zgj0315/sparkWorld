package org.after90.bangcle

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Analyzer {

  case class JFJQ(date: String, time: String, imsi: String, imei: String, sdk_version: String, udid: String)

  case class JFStart(date: String, sdk_version: String, imei: String, imsi: String)

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

  case class UDID_IMEI(udid: String, imei: String)

  def main(args: Array[String]): Unit = {
    //    v3()
    //    v4()
    //    jf()
    analyzer()
  }

  /**
    * 3.0版本数据
    */
  def v3(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val devFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/dev/")
    val devDF = devFile
      .map(_.split("\t"))
      .map(x => EDevV3(x(4).trim, x(18).trim, x(19).trim, x(20).trim, x(21).trim, x(22).trim, x(25).trim, x(27).trim, x(28).trim, x(29).trim))
      .toDF()
    devDF.createOrReplaceTempView("dev")
    var sqlDF = spark.sql("SELECT distinct * FROM dev")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/output_dev")

    val startFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v3.0/start/20190809/")
    val startDF = startFile
      .map(_.split(","))
      .map(x => EStartV3(x(4).trim, x(12).trim, x(19).trim, x(23).trim, x(24).trim, x(25).trim, x(26).trim))
      .toDF()
    startDF.createOrReplaceTempView("start")
    sqlDF = spark.sql("select distinct * from start")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v3.0/output_start")
    spark.stop()
  }


  /**
    * 4.0版本数据
    */
  def v4(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val devFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/dev/*.txt")
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
    devDF.createOrReplaceTempView("dev")

    var sqlDF = spark.sql("SELECT distinct * FROM dev")
    //    sqlDF.show()
    //    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/output_dev")

    val startFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/v4.3/start/*.txt")
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
    startDF.createOrReplaceTempView("start")

    sqlDF = spark.sql("SELECT distinct * FROM start")
    //    sqlDF.show()
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/v4.3/output_start")
    spark.stop()
  }

  /**
    * 计费数据
    */
  def jf(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/jianquan/*.txt")
    val jqDF = jqFile
      .map(_.split("\t"))
      .filter(_.size == 6)
      .map(x => JFJQ(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jqDF.createOrReplaceTempView("jq")
    var sqlDF = spark.sql("SELECT distinct * FROM jq")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/output_jq")

    val startFile = spark.sparkContext.textFile("/Volumes/HDD01/bangcle/jifei/start/*.txt")
    val startDF = startFile
      .map(_.split("\t"))
      .filter(_.size == 4)
      .map(x => JFStart(x(0).trim, x(1).trim, x(2).trim, x(3).trim))
      .toDF()
    startDF.createOrReplaceTempView("start")
    sqlDF = spark.sql("SELECT distinct * FROM start")
    sqlDF.repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/jifei/output_start")

    spark.stop()
  }

  def analyzer(): Unit = {
    val conf = new SparkConf().setAppName("Bangcle").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val jqFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/jf/output_jq/part*")
    val jqDF = jqFile
      .map(_.split(","))
      .filter(_.size == 6)
      .map(x => JFJQ(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim))
      .toDF()
    jqDF.createOrReplaceTempView("jf_jq")

    val jfStartFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/jf/output_start/part*")
    val jfStartDF = jfStartFile
      .map(_.split(","))
      .filter(_.size == 4)
      .map(x => JFStart(x(0).trim, x(1).trim, x(2).trim, x(3).trim))
      .toDF()
    jfStartDF.createOrReplaceTempView("jf_start")

    val eDevV3File = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/v3/output_dev/part*")
    val eDevV3DF = eDevV3File
      .map(_.split(","))
      .filter(_.size == 10)
      .map(x => EDevV3(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim, x(7).trim, x(8).trim, x(9).trim))
      .toDF()
    eDevV3DF.createOrReplaceTempView("dev_v3")

    val eStartV3File = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/v3/output_start/part*")
    val eStartV3DF = eStartV3File
      .map(_.split(","))
      .filter(_.size == 7)
      .map(x => EStartV3(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim))
      .toDF()
    eStartV3DF.createOrReplaceTempView("start_v3")

    val eDevV4File = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/v4/output_dev/part*")
    val eDevV4DF = eDevV4File
      .map(_.split(","))
      .filter(_.size == 10)
      .map(x => EDevV4(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim, x(7).trim, x(8).trim, x(9).trim))
      .toDF()
    eDevV4DF.createOrReplaceTempView("dev_v4")

    val eStartV4File = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/v4/output_start/part*")
    val eStartV4DF = eStartV4File
      .map(_.split(","))
      .filter(_.size == 5)
      .map(x => EStartV4(x(0).trim, x(1).trim, x(2).trim, x(3).trim, x(4).trim))
      .toDF()
    eStartV4DF.createOrReplaceTempView("start_v4")


    //    var sqlDF = spark.sql("SELECT * FROM jf_jq")
    //    sqlDF.show()
    //    +--------+--------------+---------------+---------------+-----------+--------------------+
    //    |    date|          time|           imsi|           imei|sdk_version|                udid|
    //    +--------+--------------+---------------+---------------+-----------+--------------------+
    //    |20190809|20190809225505|460028077077112|862322046237579|     3.5.16|f0961804-0c2d-4cc...|
    //    |20190809|20190809225014|460022346585487|861203041312263|     3.5.16|018b0221-18bd-481...|

    //    sqlDF = spark.sql("select * from jf_start")
    //    sqlDF.show()
    //    +--------+-----------+---------------+---------------+
    //    |    date|sdk_version|           imei|           imsi|
    //    +--------+-----------+---------------+---------------+
    //    |20190809|      3.5.0|862887033976637|460008551746075|
    //    |20190809|    3.2.1.5|869198031098298|460026923445966|


    //    sqlDF = spark.sql("select * from dev_v3")
    //    sqlDF.show()
    //    +--------------------+---------------+------------+-----------------+-------------+-------------+---------------+-----------+----------+--------------------+
    //    |                udid|           imei|manufacturer|manufacturer_prop|        model|   model_prop|           imsi|sdk_version|os_version|             os_name|
    //    +--------------------+---------------+------------+-----------------+-------------+-------------+---------------+-----------+----------+--------------------+
    //    |4630e45f-9b1e-3fb...|868054035035193|        OPPO|             OPPO|     OPPO A73|     OPPO A73|460077390009957|         25|     7.1.1|  A73_11_A.41_190613|
    //    |44f086fc-f9f1-307...|           null|        OPPO|             OPPO|     OPPO A77|     OPPO A77|           null|         25|     7.1.1|  A77_11_A.22_190528|


    //    sqlDF = spark.sql("select * from start_v3")
    //    sqlDF.show()
    //    +--------------------+------+--------------------+-----------------+---------------+------------+------------+
    //    |                udid|market|               pname|manu_manufacturer|manu_os_version|apk_app_name|apk_ver_name|
    //    +--------------------+------+--------------------+-----------------+---------------+------------+------------+
    //    |1d28f62a-2805-345...|    官方|com.kiloo.subwaysurf|              TCL|          4.4.4|        地铁跑酷|      2.74.0|
    //    |f2af0293-eb56-350...|    官方|     com.pfu.popstar|             alps|          4.4.2|   消消星星乐：最新版|       8.2.3|


    //    sqlDF = spark.sql("select * from dev_v4")
    //    sqlDF.show()
    //    +--------------------+---------------+------------+-----------------+---------------+---------------+---------------+-----------+----------+--------------------+
    //    |                udid|           imei|manufacturer|manufacturer_prop|          model|     model_prop|           imsi|sdk_version|os_version|             os_name|
    //    +--------------------+---------------+------------+-----------------+---------------+---------------+---------------+-----------+----------+--------------------+
    //    |b8582f4e-2643-3c5...|863883033536009|        vivo|             vivo|       vivo Y67|       vivo Y67|460005180493399|         23|       6.0| MRA58K release-keys|
    //    |5ae66b50-c450-3cb...|867230034792528|      HUAWEI|           HUAWEI|       LDN-TL00|       LDN-TL00|460021466080508|         26|     8.0.0|LDN-TL00 8.0.0.22...|


    //    sqlDF = spark.sql("select * from start_v4")
    //    sqlDF.show()
    //    +--------------------+------+--------------------+----------+-----------+
    //    |                udid|market|               pname|  app_name|app_version|
    //    +--------------------+------+--------------------+----------+-----------+
    //    |da533a07-8939-39d...|    官方|com.zengame.zrttd...|天天斗地主（真人版）|   5.60.384|
    //    |944c3fb6-52d5-35a...|    官方|com.zengame.waddz...|天天斗地主（真人版）|   5.60.380|

    //计费数据自身分析
    //    sqlDF = spark.sql("select count(*) from jf_jq")
    //    sqlDF.show()
    //    1227354
    //    sqlDF = spark.sql("select count(*) from jf_start")
    //    sqlDF.show()
    //    4597804

    //    sqlDF = spark.sql("select count(distinct imei) from jf_jq")
    //    sqlDF.show()
    //    168500

    //    sqlDF = spark.sql("select count(distinct imei) from jf_start")
    //    sqlDF.show()
    //    4206482

    //鉴权和启动交集
    //    sqlDF = spark.sql("select count(distinct a.imei) from jf_jq as a, jf_start as b where a.imei = b.imei")
    //    sqlDF.show()
    //    166730 98.95

    //计费鉴权和v3
    //udid交集
    //    sqlDF = spark.sql("select count(distinct udid) from jf_jq")
    //    sqlDF.show()
    //    1142532

    //    sqlDF = spark.sql("select count(distinct a.udid) from jf_jq as a, dev_v3 as b where a.udid = b.udid")
    //    sqlDF.show()
    //    2908 0.25%
    //    sqlDF = spark.sql("select b.os_version from jf_jq as a, dev_v3 as b where a.udid = b.udid")
    //    sqlDF.groupBy("os_version").count().sort($"count".desc).repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/tmp/output")
    //    sqlDF = spark.sql("select os_version from dev_v3")
    //    sqlDF.groupBy("os_version").count().sort($"count".desc).repartition(1).write.format("csv").save("/Volumes/HDD01/bangcle/tmp/output")
    // 结论，交集的设备版本均匀分布

    //计费启动数据和ev3启动交集
    //    sqlDF = spark.sql("select count(distinct a.imei) from dev_v3 as a, jf_start as b where a.imei = b.imei")
    //    sqlDF.show()
    //    143266 3.41%
    //    sqlDF = spark.sql("select a.os_version from dev_v3 as a, jf_start as b where a.imei = b.imei")
    //    sqlDF.groupBy("os_version").count().sort($"count".desc).show()

    //计费启动数据和ev4启动交集
    //    sqlDF = spark.sql("select count(distinct a.imei) from dev_v4 as a, jf_start as b where a.imei = b.imei")
    //    sqlDF.show()
    //    1042605 24.79%

    //    sqlDF = spark.sql("select * from jf_start")
    //    sqlDF.groupBy("imei").count().sort($"count".desc).show(100)
    //    sqlDF.groupBy("imei").count().groupBy("count").count().sort($"count".desc).show(100)
    //    sqlDF = spark.sql("select count(distinct imei) from jf_jq where imei in (select imei from dev_v3)")
    //    sqlDF = spark.sql("select count(distinct udid) from jf_start where udid in (select udid from dev_v4)")
    //    sqlDF.show()

    //    计费鉴权数据和威胁感知数据有交集的设备，imei是否对应？
    //    sqlDF = spark.sql("select count(distinct (a.udid, a.imei as imei_jq, b.imei as imei_dev)) from jf_jq as a, dev_v3 as b where a.udid = b.udid")
    //    sqlDF.show()
    //    匹配数据条数
    //    3583
    //    sqlDF = spark.sql("select count(distinct (a.udid, a.imei as imei_jq, b.imei as imei_dev)) from jf_jq as a, dev_v3 as b where a.udid = b.udid and a.imei = b.imei")
    //    sqlDF.show()
    //    一一对应的数据
    //    2818 78.65%

    //    sqlDF = spark.sql("select count(distinct (a.udid, a.imei as imei_jq, b.imei as imei_dev)) from jf_jq as a, dev_v3 as b where a.imei = b.imei")
    //    sqlDF.show()
    //    23457
    //    sqlDF = spark.sql("select distinct a.udid, a.imei as imei_jq, b.imei as imei_dev from jf_jq as a, dev_v3 as b where a.udid = b.udid and a.imei <> b.imei")
    //    sqlDF.show()
    //    sqlDF.repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/join/output")

    //    val sqlDF = spark.sql("select * from jf_jq where imei in (select imei from dev_v3)")
    //    sqlDF.groupBy("imei").count().sort($"count".desc).show(100)
    val sqlDF = spark.sql("select udid, imei from jf_jq where udid in (select udid from dev_v3)")
    sqlDF.repartition(1).write.format("csv").save("/Users/zhaogj/bangcle/southbase/output/jq_udid_imei")
    val tmpFile = spark.sparkContext.textFile("/Users/zhaogj/bangcle/southbase/output/jq_udid_imei/part*")
    val tmpDF = tmpFile
      .map(_.split(","))
      .filter(_.size == 2)
      .map(x => UDID_IMEI(x(0).trim, x(1).trim))
      .toDF()
    tmpDF.createOrReplaceTempView("udid_imei")
    //    val sqlDF = spark.sql("(select udid, count(imei) from udid_imei group by udid) as a, (select udid, count(*) from () group by udid) as b")
    //    sqlDF.show()

    spark.stop()
  }
}
