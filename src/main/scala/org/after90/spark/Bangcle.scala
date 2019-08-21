package org.after90.spark

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 采用rdd的方式统计设备udid,imei数据
  * spark-submit \
  * --class org.after90.spark.Bangcle \
  * --driver-memory 1g \
  * --executor-memory 1g \
  * --executor-cores 2 \
  * --jars fastjson-1.2.59.jar \
  * sparkworld_2.10-0.1.0.jar \
  * yarn-client \
  * hdfs:///bangcle/bangcle_dataservice/data/message/message*.gz \
  * hdfs:///bangcle/zhaogj/output
  */
object Bangcle {
  def main(args: Array[String]): Unit = {
    getUdid(args)
  }

  def getUdid(args: Array[String]): Unit = {
    var master = "local[*]"
    var inputPath = "/Volumes/HDD01/bangcle/aliyun/hdfs/message_data_20190805_1564969723942_1565231757484"
    var outputPath = "/Volumes/HDD01/bangcle/aliyun/output"
    if (args.length == 3) {
      master = args(0)
      inputPath = args(1)
      outputPath = args(2)
    }
    val conf = new SparkConf().setAppName("Bangcle").setMaster(master)
    val sc = new SparkContext(conf)
    val srcFile = sc.textFile(inputPath)
    srcFile.map(_.split("\t"))
      .filter(_.size == 3)
      .map(x => JSON.parseObject(x(2).trim))
      .map(_.getJSONObject("body"))
      .filter(_.containsKey("udid"))
      .map(x => {
        (x.getString("udid"), 1)
      })
      .reduceByKey((x, y) => x + y)
      .keys
      .repartition(1)
      .saveAsTextFile(outputPath)
    sc.stop()
  }
}
