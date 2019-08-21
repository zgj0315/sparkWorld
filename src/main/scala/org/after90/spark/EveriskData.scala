package org.after90.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采用rdd的方式统计设备udid,imei数据
  * spark-submit --jars fastjson-1.2.59.jar --class org.after90.spark.EveriskData sparkworld_2.10-0.1.0.jar local[*] /bangcle/bangcle_dataservice/data/message/message_data_20190621_1561079401417_1561079744157.gz /bangcle/zhaogj/output
  * spark-submit --jars fastjson-1.2.59.jar --class org.after90.spark.EveriskData sparkworld_2.10-0.1.0.jar local[*] /bangcle/bangcle_dataservice/data/message/ /bangcle/zhaogj/output
  */
object EveriskData {
  def main(args: Array[String]): Unit = {
    var master = "local[*]"
    var srcPath = "/Users/zhaogj/bangcle/everisk/src/message_data_20190805_1564969723942_1565231757484"
    var outputPath = "/Users/zhaogj/tmp/output";
    if (args.length == 3) {
      master = args(0)
      srcPath = args(1)
      outputPath = args(2)
    }
    val conf = new SparkConf().setAppName("EveriskData").setMaster(master)
    val sc = new SparkContext(conf)
    val srcFile = sc.textFile(srcPath)
    srcFile.map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(x => {
        var body = x.getJSONObject("body")
        body.put("server_time", x.getLong("server_time"))
        body
      })
      .filter(_.containsKey("udid"))
      .map(x => {
        (x.getString("udid"), 1)
      })
      .reduceByKey((x, y) => x + y)
      .keys
      .saveAsTextFile(outputPath)
    sc.stop()
  }
}
