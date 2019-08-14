package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhaogj on 30/03/2017.
  */
object ExceptionHandlingTest {
  def main(args: Array[String]) {
    val strAppName = "InitSparkAndSession"
    //方便调试，如果运行在集群上，需要将master设置为yarn-cluster
    var strMaster = "local[*]"
    if (args.length == 1) {
      strMaster = args(0)
    }
    //必须先初始化sc,后初始化session,奇了个怪了
    val spark = SparkSession
      .builder()
      .master(strMaster)
      .appName(strAppName)
      .getOrCreate()

    spark.sparkContext.parallelize(0 until spark.sparkContext.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }

    spark.stop()
  }
}
