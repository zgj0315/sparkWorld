package org.after90.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 30/03/2017.
  * spark2.0里初始化sc已经不是这么玩了，这里只是做个纪念罢了
  */
object InitializingSpark {
  def main(args: Array[String]): Unit = {
    val strAppName = "strAppName"
    val strMaster = "local[*]"
    val conf = new SparkConf().setAppName(strAppName).setMaster(strMaster)
    val sc = new SparkContext(conf)

    sc.stop()
  }
}
