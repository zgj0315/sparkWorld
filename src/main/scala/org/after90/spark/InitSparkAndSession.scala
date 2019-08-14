package org.after90.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 30/03/2017.
  */
object InitSparkAndSession {
  def main(args: Array[String]): Unit = {
    val strAppName = "InitSparkAndSession"
    //方便调试，如果运行在集群上，需要将master设置为yarn-cluster
    var strMaster = "local[*]"
    if (args.length == 1) {
      strMaster = args(0)
    }
    val spark = SparkSession
      .builder()
      .master(strMaster)
      .appName(strAppName)
      .getOrCreate()
    val sc = spark.sparkContext

    sc.stop()
    spark.stop()
  }
}
